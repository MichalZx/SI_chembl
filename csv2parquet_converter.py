import pandas as pd
from pathlib import Path

CSV_PATH = Path("Dane/chembl_activity_subset_01.csv")
PARQUET_PATH = Path("Dane/chembl_activity_subset_01.parquet")

CHUNK_SIZE = 500_000

def csv_to_parquet():
    first = True
    total = 0

    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
        for col in [
            "standard_value", "pchembl_value", "mw_freebase",
            "alogp", "hbd", "hba", "psa", "heavy_atoms",
            "confidence_score"
        ]:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        if first:
            chunk.to_parquet(
                PARQUET_PATH,
                engine="pyarrow",
                compression="snappy",
                index=False
            )
            first = False
        else:
            # Parquet -> zapis jako dataset
            chunk.to_parquet(
                PARQUET_PATH.parent / "parquet_parts",
                engine="pyarrow",
                compression="snappy",
                index=False
            )

        total += len(chunk)
        print(f"✔ przekonwertowano {total:,} rekordów")

    print(f"\nDONE. Parquet ready.")

if __name__ == "__main__":
    csv_to_parquet()