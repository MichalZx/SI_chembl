import pandas as pd
import numpy as np
from pathlib import Path
from rdkit import Chem
from rdkit.Chem import Descriptors, Lipinski, Crippen, rdMolDescriptors

INPUT_PATH = Path("Dane/chembl_activity_cleaned_00.parquet")
OUTPUT_PATH = Path("Dane/chembl_ml_dataset_00.parquet")

ACTIVITY_THRESHOLD_NM = 1000  # do klasyfikacji (1 µM)

def compute_pic50(nm_value):
    molar = nm_value * 1e-9
    return -np.log10(molar)

def compute_descriptors(smiles):
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None
    return {
        "MolWt": Descriptors.MolWt(mol),
        "LogP": Crippen.MolLogP(mol),
        "TPSA": rdMolDescriptors.CalcTPSA(mol),
        "HBD": Lipinski.NumHDonors(mol),
        "HBA": Lipinski.NumHAcceptors(mol),
        "HeavyAtoms": mol.GetNumHeavyAtoms(),
        "RotatableBonds": Lipinski.NumRotatableBonds(mol),
        "AromaticRings": rdMolDescriptors.CalcNumAromaticRings(mol),
        "FractionCSP3": rdMolDescriptors.CalcFractionCSP3(mol),
    }

def run_feature_engineering():
    print("Loading data...")
    df = pd.read_parquet(INPUT_PATH)

    # --- target: pIC50 ---
    print("Computing pIC50...")
    df["pIC50"] = df["standard_value"].apply(compute_pic50)

    # --- classification label ---
    print("Generating activity class...")
    df["is_active"] = (df["standard_value"] <= ACTIVITY_THRESHOLD_NM).astype(int)

    # --- descriptors ---
    print("Computing RDKit descriptors...")
    descriptor_list = []

    for smi in df["canonical_smiles"]:
        desc = compute_descriptors(smi)
        descriptor_list.append(desc)

    desc_df = pd.DataFrame(descriptor_list)
    mask_valid = desc_df.notna().all(axis=1)
    df = df.loc[mask_valid].reset_index(drop=True)
    desc_df = desc_df.loc[mask_valid].reset_index(drop=True)
    df = pd.concat([df, desc_df], axis=1)

    # usuń niepotrzebne kolumny
    drop_cols = [
        "canonical_smiles",
        "activity_id",
        "assay_id",
        "doc_id",
        "record_id"
    ]

    for col in drop_cols:
        if col in df.columns:
            df = df.drop(columns=col)

    print("Saving ML dataset...")
    df.to_parquet(
        OUTPUT_PATH,
        engine="pyarrow",
        compression="snappy",
        index=False
    )
    print("Done.")
    print("Final shape:", df.shape)

if __name__ == "__main__":
    run_feature_engineering()