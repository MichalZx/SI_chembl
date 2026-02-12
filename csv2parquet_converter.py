import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

CSV_PATH = Path("Dane/chembl_activity_subset_03.csv")
PARQUET_PATH = Path("Dane/chembl_activity_subset_03.parquet")

CHUNK_SIZE = 500_000

def csv_to_parquet():
    writer = None
    total = 0

    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):

        for col in [
            "activity_id",
            "record_id",
            "molregno",
            "src_id",
            "toid",
            "standard_value",
            "upper_value",
            "standard_upper_value",
            "pchembl_value",
            "confidence_score",
            "mw_freebase",
            "alogp",
            "hbd",
            "hba",
            "psa",
            "heavy_atoms",
            "qed_weighted",
            "num_ro5_violations",
            "max_phase",
            "first_approval",
            "potential_duplicate"
        ]:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        table = pa.Table.from_pandas(chunk)

        if writer is None:
            writer = pq.ParquetWriter(
                PARQUET_PATH,
                table.schema,
                compression="snappy"
            )

        writer.write_table(table)

        total += len(chunk)
        print(f"✔ przekonwertowano {total:,} rekordów")
    if writer:
        writer.close()

    print("\nDONE. Single parquet file ready.")

if __name__ == "__main__":
    csv_to_parquet()