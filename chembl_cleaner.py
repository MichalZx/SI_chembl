import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

INPUT_PATH = Path("Dane/chembl_activity_subset_01.parquet")
OUTPUT_DIR = Path("Dane")
OUTPUT_DIR.mkdir(exist_ok=True)
CHUNK_SIZE = 500_000

def clean_chunk(df: pd.DataFrame) -> pd.DataFrame:

    numeric_cols = [
        "standard_value",
        "pchembl_value",
        "mw_freebase",
        "alogp",
        "hbd",
        "hba",
        "psa",
        "heavy_atoms",
        "confidence_score"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["standard_value", "canonical_smiles"])
    df = df[df["standard_value"] > 0]

    if "confidence_score" in df.columns:
        df = df[df["confidence_score"] >= 7]

    if "potential_duplicate" in df.columns:
        df = df[df["potential_duplicate"] != 1]

    df = df.drop_duplicates(subset=["activity_id"])

    return df


def clean_parquet():
    parquet_file = pq.ParquetFile(INPUT_PATH)

    total = 0

    for i in range(parquet_file.num_row_groups):
        table = parquet_file.read_row_group(i)
        df = table.to_pandas()

        cleaned = clean_chunk(df)

        out_file = OUTPUT_DIR / f"chembl_activity_cleaned_{i:02d}.parquet"
        cleaned.to_parquet(
            out_file,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        total += len(cleaned)
        print(f"✔ row_group {i:05d} | cleaned rows: {total:,}")

    print("\nDONE.")
    print(f"Clean dataset: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    clean_parquet()