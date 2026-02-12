import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("../POBRANE/chembl_36/chembl_36_sqlite/chembl_36.db")
CSV_PATH = Path("Dane/chembl_activity_subset_03.csv")
# PARQUET_PATH = Path(OUT_PATH+".parquet")

CHUNK_SIZE = 100_000
MIN_CONFIDENCE = 7
STANDARD_TYPES = ("IC50", "Ki", "Kd", "EC50")

# =========================
# ZAPYTANIE SQL
# =========================
QUERY = f"""
SELECT
    A.activity_id,
    A.assay_id as assay_chembl_id,
    A.doc_id,
    A.record_id,
    A.molregno,
    MD.chembl_id as molecule_chembl_id,
    TD.chembl_id as target_chembl_id,
    A.standard_relation,
    A.standard_value,
    A.standard_units,
    A.standard_flag,
    A.standard_type,
    A.potential_duplicate,
    A.pchembl_value,
    A.bao_endpoint,
    A.uo_units,
    A.qudt_units,
    A.toid,
    A.upper_value,
    A.standard_upper_value,
    A.src_id,
    A.type,
    A.relation,
    A.value,
    A.units,
    A.text_value,
    A.standard_text_value,
    ASA.confidence_score,
    AT.assay_desc as  assay_type_description,
    CS.canonical_smiles,
    CP.mw_freebase,
    CP.alogp,
    CP.hbd,
    CP.hba,
    CP.psa,
    CP.heavy_atoms,
    CP.qed_weighted,
    CP.num_ro5_violations,
    MD.max_phase,
    MD.first_approval,
    TD.pref_name AS target_name,
    TD.organism,
    TD.target_type
FROM activities A
JOIN assays ASA
    ON A.assay_id = ASA.assay_id
JOIN assay_type AT
    ON ASA.assay_type = AT.assay_type
JOIN compound_structures CS
    ON CS.molregno = A.molregno
JOIN compound_properties CP
    ON CP.molregno = A.molregno
JOIN molecule_dictionary MD
    ON MD.molregno = A.molregno
JOIN target_dictionary TD
    ON ASA.tid = TD.tid
WHERE
    A.standard_value IS NOT NULL
    AND ASA.confidence_score >= {MIN_CONFIDENCE}
    AND A.standard_type IN {STANDARD_TYPES}
    AND A.relation = "="
"""

# =========================
# EKSTRAKCJA
# =========================
def extract_to_csv():
    conn = sqlite3.connect(DB_PATH)
    first = not CSV_PATH.exists()
    total = 0

    for chunk in pd.read_sql_query(QUERY, conn, chunksize=CHUNK_SIZE):
        # sanity: typy liczbowe
        for col in [
            "standard_value", "pchembl_value", "mw_freebase",
            "alogp", "hbd", "hba", "psa", "heavy_atoms",
            "confidence_score"
        ]:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        chunk.to_csv(
            CSV_PATH,
            mode="a",
            header=first,
            index=False
        )

        total += len(chunk)
        first = False
        print(f"✔ zapisano {total:,} rekordów")

    conn.close()
    print(f"\nDONE. CSV: {CSV_PATH.resolve()}")

if __name__ == "__main__":
    extract_to_csv()
    # conn = sqlite3.connect(DB_PATH)
    # print(pd.read_sql_query("SELECT COUNT(*) FROM activities", conn))
    # conn.close()