import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

INPUT_PATH = Path("Dane/chembl_activity_subset_02.parquet")
OUTPUT_PATH = Path("Dane/chembl_activity_cleaned_02.parquet")

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

    df = df[df["canonical_smiles"].str.strip() != ""]
    df = df[df["canonical_smiles"].str.lower() != "nan"]

    if "confidence_score" in df.columns:
        df = df[df["confidence_score"] >= 7]

    if "potential_duplicate" in df.columns:
        df = df[df["potential_duplicate"] != 1]

    if "activity_id" in df.columns:
        df = df.drop_duplicates(subset=["activity_id"])

    if "standard_relation" in df.columns:
        df = df[df["standard_relation"] == "="]

    if "relation" in df.columns:
        df = df[df["relation"] == "="]

    return df


def clean_parquet():
    parquet_file = pq.ParquetFile(INPUT_PATH)
    writer = None
    total = 0

    for i in range(parquet_file.num_row_groups):
        table = parquet_file.read_row_group(i)
        df = table.to_pandas()

        cleaned = clean_chunk(df)

        cleaned_table = pa.Table.from_pandas(cleaned)

        if writer is None:
            writer = pq.ParquetWriter(
                OUTPUT_PATH,
                cleaned_table.schema,
                compression="snappy"
            )

        writer.write_table(cleaned_table)

        total += len(cleaned)
        print(f"✔ row_group {i:05d} | cleaned rows: {total:,}")

    if writer:
        writer.close()

    print("\nDONE.")
    print(f"Single file: {OUTPUT_PATH.resolve()}")


if __name__ == "__main__":
    clean_parquet()