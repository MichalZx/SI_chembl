import json
import io
import base64

import streamlit as st
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import ollama

from rdkit import Chem, DataStructs
from rdkit.Chem import Draw
from rdkit.Chem.rdFingerprintGenerator import GetMorganGenerator
from torch_geometric.data import Data
from torch_geometric.nn import GINConv, global_add_pool

# ─────────────────────────────────────────────
# Konfiguracja
# ─────────────────────────────────────────────
MODEL_NAME    = "granite3.3"
GIN_PATH      = "../Dane/gin_01.pt"
MLP_PATH      = "../Dane/mlp_scaffold_01.pt"
DEVICE        = torch.device("cuda" if torch.cuda.is_available() else "cpu")
SHOW_THINKING = False   # True → pokazuje bloki <think>...</think> modelu w UI

# ─────────────────────────────────────────────
# Featuryzacja RDKit (spójna z notebookiem GIN)
# ─────────────────────────────────────────────
ATOM_LIST = [
    'C','N','O','S','F','Si','P','Cl','Br','Mg','Na','Ca',
    'Fe','As','Al','I','B','V','K','Tl','Yb','Sb','Sn',
    'Ag','Pd','Co','Se','Ti','Zn','H','Li','Ge','Cu','Au',
    'Ni','Cd','In','Mn','Zr','Cr','Pt','Hg','Pb'
]
CHIRALITY_LIST = [
    Chem.rdchem.ChiralType.CHI_UNSPECIFIED,
    Chem.rdchem.ChiralType.CHI_TETRAHEDRAL_CW,
    Chem.rdchem.ChiralType.CHI_TETRAHEDRAL_CCW,
    Chem.rdchem.ChiralType.CHI_OTHER,
]
HYBRIDIZATION_LIST = [
    Chem.rdchem.HybridizationType.S,
    Chem.rdchem.HybridizationType.SP,
    Chem.rdchem.HybridizationType.SP2,
    Chem.rdchem.HybridizationType.SP3,
    Chem.rdchem.HybridizationType.SP3D,
    Chem.rdchem.HybridizationType.SP3D2,
    Chem.rdchem.HybridizationType.OTHER,
]


def _one_hot(value, choices):
    enc = [0] * (len(choices) + 1)
    try:
        enc[choices.index(value)] = 1
    except ValueError:
        enc[-1] = 1
    return enc


def _atom_features(atom):
    feats = []
    feats += _one_hot(atom.GetSymbol(),       ATOM_LIST)
    feats += _one_hot(atom.GetChiralTag(),     CHIRALITY_LIST)
    feats += _one_hot(atom.GetDegree(),        list(range(11)))
    feats += _one_hot(atom.GetFormalCharge(),  list(range(-5, 6)))
    feats += _one_hot(atom.GetTotalNumHs(),    list(range(9)))
    feats += _one_hot(atom.GetHybridization(), HYBRIDIZATION_LIST)
    feats.append(int(atom.GetIsAromatic()))
    feats.append(int(atom.IsInRing()))
    return feats


_ATOM_DIM = len(_atom_features(Chem.MolFromSmiles('C').GetAtomWithIdx(0)))


def smiles_to_graph(smiles: str):
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    x = torch.tensor([_atom_features(a) for a in mol.GetAtoms()], dtype=torch.float)
    edge_index = []
    for bond in mol.GetBonds():
        i, j = bond.GetBeginAtomIdx(), bond.GetEndAtomIdx()
        edge_index += [[i, j], [j, i]]
    if edge_index:
        edge_index = torch.tensor(edge_index, dtype=torch.long).t().contiguous()
    else:
        edge_index = torch.zeros((2, 0), dtype=torch.long)
    return Data(x=x, edge_index=edge_index)


# ─────────────────────────────────────────────
# Modele
# ─────────────────────────────────────────────
def _make_mlp():
    """
    Architektura zgodna z MLP_scaffold.ipynb (build_mlp):
    2048 → 1024 → 512 → 128 → 1  z BatchNorm + Dropout między warstwami.
    """
    return nn.Sequential(
        nn.Linear(2048, 1024),
        nn.BatchNorm1d(1024),
        nn.ReLU(),
        nn.Dropout(0.3),

        nn.Linear(1024, 512),
        nn.BatchNorm1d(512),
        nn.ReLU(),
        nn.Dropout(0.3),

        nn.Linear(512, 128),
        nn.BatchNorm1d(128),
        nn.ReLU(),
        nn.Dropout(0.3),

        nn.Linear(128, 1),
    )


def _make_gin_mlp(in_dim, hidden_dim):
    return nn.Sequential(
        nn.Linear(in_dim, hidden_dim),
        nn.BatchNorm1d(hidden_dim),
        nn.ReLU(),
        nn.Linear(hidden_dim, hidden_dim),
    )


class GIN(nn.Module):
    def __init__(self, input_dim=_ATOM_DIM, hidden_dim=256, num_layers=5, dropout=0.2):
        super().__init__()
        self.dropout = dropout
        self.convs = nn.ModuleList()
        self.bns   = nn.ModuleList()
        for i in range(num_layers):
            in_d = input_dim if i == 0 else hidden_dim
            self.convs.append(GINConv(_make_gin_mlp(in_d, hidden_dim), train_eps=True))
            self.bns.append(nn.BatchNorm1d(hidden_dim))
        self.lin1 = nn.Linear(hidden_dim * num_layers, 128)
        self.lin2 = nn.Linear(128, 1)
        self.num_layers = num_layers

    def forward(self, x, edge_index, batch):
        outs = []
        for conv, bn in zip(self.convs, self.bns):
            x = conv(x, edge_index)
            x = bn(x)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout, training=self.training)
            outs.append(global_add_pool(x, batch))
        out = torch.cat(outs, dim=-1)
        out = F.relu(self.lin1(out))
        out = F.dropout(out, p=self.dropout, training=self.training)
        return self.lin2(out)


@st.cache_resource
def load_models():
    mlp = _make_mlp()
    mlp.load_state_dict(torch.load(MLP_PATH, map_location=DEVICE))
    mlp.eval().to(DEVICE)

    gin = GIN()
    gin.load_state_dict(torch.load(GIN_PATH, map_location=DEVICE))
    gin.eval().to(DEVICE)
    return mlp, gin


_generator = GetMorganGenerator(radius=2, fpSize=2048)


# ─────────────────────────────────────────────
# Pomocnicze: render cząsteczki
# ─────────────────────────────────────────────
def mol_image_b64(smiles: str) -> str | None:
    """Generuje PNG struktury 2D z SMILES, zwraca base64 lub None."""
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    img = Draw.MolToImage(mol, size=(320, 240))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


def extract_smiles_from_text(text: str) -> list[str]:
    """
    Wyciąga prawidłowe SMILES z tekstu.
    Separatorami są TYLKO spacje, przecinki, średniki i cudzysłowy —
    NIE nawiasy ani nawiasy kwadratowe, bo są częścią składni SMILES.
    """
    import re
    tokens = re.split(r'[\s,;"]+', text)
    found = []
    for tok in tokens:
        tok = tok.strip("'`:")
        if len(tok) < 3:
            continue
        # Token musi zawierać przynajmniej jedną cyfrę LUB typowy znak SMILES
        # (=, #, @, /, \, +, -, nawiasy) — odrzuca zwykłe słowa
        if not re.search(r'[0-9=#+\-@\\/]', tok):
            continue
        try:
            mol = Chem.MolFromSmiles(tok)
        except Exception:
            mol = None
        if mol is not None and tok not in found:
            found.append(tok)
    return found


def process_thinking(text: str) -> tuple[str, str]:
    """
    Rozdziela odpowiedź modelu na blok <think>...</think> i właściwą treść.
    Zwraca (thinking_text, answer_text).
    Jeśli model nie wygenerował bloku think, thinking_text jest pusty.
    """
    import re
    thinking = ""
    parts = re.findall(r'<think>(.*?)</think>', text, flags=re.DOTALL)
    if parts:
        thinking = "\n\n".join(p.strip() for p in parts)
    answer = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    return thinking, answer


def render_thinking(thinking: str):
    """Wyświetla blok myślenia modelu jako zwinięty expander (jeśli SHOW_THINKING=True)."""
    if not SHOW_THINKING or not thinking:
        return
    with st.expander("🧠 Myślenie modelu", expanded=False):
        st.markdown(f"```\n{thinking}\n```")


def render_smiles_images_inline(text: str, already_rendered: set | None = None):
    """
    Wyświetla struktury 2D cząsteczek znalezionych w tekście.
    already_rendered – zbiór SMILES już pokazanych w tym samym kontenerze
    (zapobiega duplikatom gdy ten sam SMILES pojawia się i w tekście i w tool_log).
    """
    smiles_list = [
        s for s in extract_smiles_from_text(text)
        if already_rendered is None or s not in already_rendered
    ]
    if not smiles_list:
        return
    if already_rendered is not None:
        already_rendered.update(smiles_list)

    cols = st.columns(min(len(smiles_list), 4))
    for col, smi in zip(cols, smiles_list):
        with col:
            if smi not in st.session_state.mol_images:
                b64 = mol_image_b64(smi)
                if b64:
                    st.session_state.mol_images[smi] = b64
            if smi in st.session_state.mol_images:
                st.image(
                    f"data:image/png;base64,{st.session_state.mol_images[smi]}",
                    caption=smi[:38] + ("…" if len(smi) > 38 else ""),
                    use_container_width=True,
                )


# ─────────────────────────────────────────────
# Funkcje narzędzi (wywoływane przez agenta)
# ─────────────────────────────────────────────
def tool_predict_gin(smiles: str) -> dict:
    """Predykcja pChEMBL modelem GIN."""
    _, gin = load_models()
    graph = smiles_to_graph(smiles)
    if graph is None:
        return {"error": f"Nieprawidłowy SMILES: {smiles}"}
    if graph.edge_index.shape[1] == 0:
        return {"error": "Cząsteczka nie ma wiązań."}
    graph.batch = torch.zeros(graph.num_nodes, dtype=torch.long)
    graph = graph.to(DEVICE)
    with torch.no_grad():
        pred = gin(graph.x, graph.edge_index, graph.batch)
    val = float(pred.item())
    return {
        "model":    "GIN",
        "smiles":   smiles,
        "pchembl":  round(val, 4),
        "ic50_nM":  round(10 ** (9 - val), 2),
        # sygnał dla UI – zawsze dołącz obraz
        "_render_mol": True,
    }


def tool_predict_mlp(smiles: str) -> dict:
    """Predykcja pChEMBL modelem MLP."""
    mlp, _ = load_models()
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return {"error": f"Nieprawidłowy SMILES: {smiles}"}
    fp = _generator.GetFingerprint(mol)
    arr = np.zeros((2048,), dtype=np.float32)
    DataStructs.ConvertToNumpyArray(fp, arr)
    x = torch.tensor(arr, dtype=torch.float32).unsqueeze(0).to(DEVICE)
    with torch.no_grad():
        pred = mlp(x)
    val = float(pred.item())
    return {
        "model":    "MLP",
        "smiles":   smiles,
        "pchembl":  round(val, 4),
        "ic50_nM":  round(10 ** (9 - val), 2),
        "_render_mol": True,
    }


def tool_validate_smiles(smiles: str) -> dict:
    """Sprawdza poprawność SMILES i zwraca właściwości cząsteczki."""
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return {"valid": False, "smiles": smiles}
    from rdkit.Chem import Descriptors, rdMolDescriptors
    return {
        "valid":        True,
        "smiles":       smiles,
        "num_atoms":    mol.GetNumAtoms(),
        "num_bonds":    mol.GetNumBonds(),
        "mol_weight":   round(Descriptors.MolWt(mol), 2),
        "num_rings":    rdMolDescriptors.CalcNumRings(mol),
        "num_aromatic": rdMolDescriptors.CalcNumAromaticRings(mol),
        "formula":      rdMolDescriptors.CalcMolFormula(mol),
        "_render_mol":  True,
    }


def tool_draw_molecule(smiles: str) -> dict:
    """
    Generuje i zwraca obraz (strukturę 2D) cząsteczki na podstawie SMILES.
    Używaj gdy użytkownik prosi o pokazanie/narysowanie/wyświetlenie molekuły.
    """
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return {"error": f"Nieprawidłowy SMILES: {smiles}"}
    from rdkit.Chem import rdMolDescriptors
    return {
        "smiles":      smiles,
        "formula":     rdMolDescriptors.CalcMolFormula(mol),
        "_render_mol": True,
        "message":     "Obraz molekuły wyrenderowany przez RDKit.",
    }


# ─────────────────────────────────────────────
# Definicje narzędzi dla Ollama
# ─────────────────────────────────────────────
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "predict_gin",
            "description": (
                "Przewiduje biologiczną aktywność cząsteczki (wartość pChEMBL i IC50) "
                "używając modelu GIN (Graph Isomorphism Network) – model grafowy z "
                "featuryzacją RDKit, wytrenowany na CHEMBL2147. Zalecany jako domyślny. "
                "Wyższe pChEMBL = silniejsza aktywność."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "smiles": {"type": "string", "description": "SMILES cząsteczki"}
                },
                "required": ["smiles"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "predict_mlp",
            "description": (
                "Przewiduje biologiczną aktywność (pChEMBL / IC50) modelem MLP "
                "opartym na odciskach palców Morgana (2048 bitów). Szybszy, prostszy baseline."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "smiles": {"type": "string", "description": "SMILES cząsteczki"}
                },
                "required": ["smiles"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "validate_smiles",
            "description": (
                "Sprawdza poprawność zapisu SMILES i zwraca właściwości fizykochemiczne: "
                "wzór sumaryczny, masę molową, liczbę atomów, pierścieni itp."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "smiles": {"type": "string", "description": "SMILES do walidacji"}
                },
                "required": ["smiles"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "draw_molecule",
            "description": (
                "Rysuje i wyświetla strukturę 2D cząsteczki na podstawie SMILES. "
                "Użyj gdy użytkownik prosi o pokazanie, narysowanie lub wyświetlenie "
                "obrazu/struktury/wzoru cząsteczki (show molecule, draw, display structure, "
                "pokaż cząsteczkę, narysuj strukturę itp.)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "smiles": {"type": "string", "description": "SMILES cząsteczki do narysowania"}
                },
                "required": ["smiles"],
            },
        },
    },
]

TOOL_DISPATCH = {
    "predict_gin":     tool_predict_gin,
    "predict_mlp":     tool_predict_mlp,
    "validate_smiles": tool_validate_smiles,
    "draw_molecule":   tool_draw_molecule,
}

SYSTEM_PROMPT = """Jesteś asystentem do analizy i przewidywania aktywności biologicznej cząsteczek.
Masz dostęp do narzędzi lokalnych:

1. **predict_gin** – GIN (Graph Isomorphism Network), model grafowy z featuryzacją RDKit.
   Trenowany na danych ChEMBL, target CHEMBL2147 (kinaza). ZALECANY domyślny.
2. **predict_mlp** – MLP na odciskach Morgana. Szybszy baseline.
3. **validate_smiles** – walidacja SMILES + właściwości fizykochemiczne.
4. **draw_molecule** – rysuje i wyświetla strukturę 2D cząsteczki.
   ZAWSZE używaj tego narzędzia gdy użytkownik pyta o obraz/strukturę/wzór/rysunek cząsteczki.
   NIE mów że nie możesz wyświetlić obrazów – masz do tego narzędzie.

Zasady:
- Gdy widzisz SMILES → od razu wywołaj validate_smiles i predict_gin.
- Gdy użytkownik prosi o obraz/rysunek/strukturę → wywołaj draw_molecule.
- Interpretacja pChEMBL: > 6 dobra, > 7 bardzo dobra, > 8 doskonała aktywność.
- IC50 [nM] = 10^(9 - pChEMBL).
- Odpowiadaj w języku użytkownika (PL/EN).
- Nie czekaj na potwierdzenie – działaj natychmiast."""


# ─────────────────────────────────────────────
# Agent loop
# ─────────────────────────────────────────────
def run_agent(messages: list) -> tuple[str, str, list]:
    """Zwraca (thinking, answer, tool_calls_log)."""
    tool_calls_log = []
    history = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

    while True:
        response = ollama.chat(
            model=MODEL_NAME,
            messages=history,
            tools=TOOLS,
        )
        msg = response["message"]

        if not msg.get("tool_calls"):
            thinking, answer = process_thinking(msg["content"])
            return thinking, answer, tool_calls_log

        history.append({
            "role": "assistant",
            "content": msg.get("content", ""),
            "tool_calls": msg["tool_calls"],
        })

        for tc in msg["tool_calls"]:
            fn_name = tc["function"]["name"]
            fn_args = tc["function"]["arguments"]
            if isinstance(fn_args, str):
                fn_args = json.loads(fn_args)

            fn     = TOOL_DISPATCH.get(fn_name)
            result = fn(**fn_args) if fn else {"error": f"Nieznane narzędzie: {fn_name}"}

            tool_calls_log.append({"tool": fn_name, "args": fn_args, "result": result})

            # Usuń wewnętrzny sygnał UI przed wysłaniem do modelu
            result_for_llm = {k: v for k, v in result.items() if not k.startswith("_")}
            history.append({
                "role":    "tool",
                "name":    fn_name,
                "content": json.dumps(result_for_llm, ensure_ascii=False),
            })


# ─────────────────────────────────────────────
# Render bloku narzędzia (JSON + obraz)
# ─────────────────────────────────────────────
def render_tool_log(log: dict, expanded: bool = False):
    tool   = log["tool"]
    args   = log["args"]
    result = log["result"]
    smi    = args.get("smiles", "")
    label  = f"🔧 `{tool}({smi})`"

    with st.expander(label, expanded=expanded):
        if "error" in result:
            st.error(result["error"])
            return

        # Dane bez wewnętrznych kluczy
        display = {k: v for k, v in result.items() if not k.startswith("_")}
        render_mol = result.get("_render_mol", False)

        if render_mol and smi:
            col_json, col_img = st.columns([2, 1])
            with col_json:
                if display:
                    st.json(display)
            with col_img:
                # Pobierz z cache lub generuj
                if smi not in st.session_state.mol_images:
                    b64 = mol_image_b64(smi)
                    if b64:
                        st.session_state.mol_images[smi] = b64
                if smi in st.session_state.mol_images:
                    st.image(
                        f"data:image/png;base64,{st.session_state.mol_images[smi]}",
                        caption=smi[:40] + ("…" if len(smi) > 40 else ""),
                        use_container_width=True,
                    )
        else:
            if display:
                st.json(display)


# ─────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="BioActivity Chat",
    page_icon="🧬",
    layout="centered",
)

st.title("🧬 Biological Activity – Chat")
st.caption(f"Powered by **{MODEL_NAME}** + GIN / MLP · target: CHEMBL2147 (kinaza)")

# ── Session state ──
for key, default in [
    ("messages",      []),
    ("tool_logs",     []),   # lista list[dict], jedna per wiadomość asystenta
    ("thinking_logs", []),   # lista str, thinking per wiadomość asystenta
    ("mol_images",    {}),   # smiles → b64 PNG (cache)
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ── Historia czatu ──
# tool_logs[i] odpowiada messages[i] (tylko dla roli assistant mają wartość)
for i, msg in enumerate(st.session_state.messages):
    role = msg["role"]
    if role == "user":
        with st.chat_message("user"):
            st.write(msg["content"])
            render_smiles_images_inline(msg["content"])
    elif role == "assistant":
        with st.chat_message("assistant", avatar="🧬"):
            thinking = st.session_state.thinking_logs[i] if i < len(st.session_state.thinking_logs) else ""
            render_thinking(thinking)
            st.markdown(msg["content"])
            logs = st.session_state.tool_logs[i] if i < len(st.session_state.tool_logs) else []
            rendered_in_tools: set[str] = {
                log["args"].get("smiles", "") for log in logs if "smiles" in log.get("args", {})
            }
            render_smiles_images_inline(msg["content"], already_rendered=rendered_in_tools)
            for log in logs:
                render_tool_log(log, expanded=False)


# ── Chat input (obsługa prefill z sidebar) ──
placeholder = "Wpisz SMILES lub pytanie, np. 'Oceń aktywność: CC1=CC=C(C=C1)NC2=...'"
user_input = st.chat_input(placeholder, key="chat_input")

# ── Obsługa wysłanej wiadomości ──
if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    st.session_state.tool_logs.append([])   # placeholder dla tej wiadomości

    with st.chat_message("user"):
        st.write(user_input)
        render_smiles_images_inline(user_input)

    with st.chat_message("assistant", avatar="🧬"):
        with st.spinner(f"{MODEL_NAME} myśli…"):
            thinking, answer, tool_logs = run_agent(st.session_state.messages)

        # Zapisz odpowiedź asystenta
        st.session_state.messages.append({"role": "assistant", "content": answer})
        st.session_state.tool_logs.append(tool_logs)
        # Uzupełnij placeholder wiadomości użytkownika (brak logów)
        st.session_state.tool_logs[-2] = []

        render_thinking(thinking)
        st.markdown(answer)
        rendered_in_tools: set[str] = {
            log["args"].get("smiles", "") for log in tool_logs if "smiles" in log.get("args", {})
        }
        render_smiles_images_inline(answer, already_rendered=rendered_in_tools)

        for log in tool_logs:
            render_tool_log(log, expanded=True)

    # Zapisz thinking równolegle do wiadomości asystenta
    # (indeks [-1] bo messages.append już się odbyło powyżej)
    st.session_state.thinking_logs.append(thinking)
    # Placeholder dla wiadomości użytkownika (brak thinking)
    if len(st.session_state.thinking_logs) < len(st.session_state.messages):
        st.session_state.thinking_logs.insert(-1, "")

    st.rerun()


# ── Sidebar ──
with st.sidebar:
    st.header("ℹ️ Modele")
    st.markdown("""
**GIN** *(Graph Isomorphism Network)*
- 5 warstw, 91-dim featuryzacja RDKit
- Scaffold split – lepsza generalizacja
- Jumping Knowledge pooling

**MLP** *(Multi-Layer Perceptron · Scaffold)*
- Morgan fingerprint 2048 bit
- Architektura: 2048→1024→512→128→1 + BatchNorm
- Trenowany ze Scaffold Split
""")

    st.divider()
    if st.button("🗑️ Wyczyść historię", use_container_width=True):
        st.session_state.messages      = []
        st.session_state.tool_logs     = []
        st.session_state.thinking_logs = []
        st.session_state.mol_images    = {}
        st.rerun()