import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from rdkit import Chem
from rdkit.Chem import Draw

# DATA_PATH = Path("Dane/chembl_activity_cleaned_00.parquet")
DATA_PATH = Path("Dane/chembl_activity_subset_02.parquet")

def basic_info(df):
    print("\n=====BASIC INFO=====")
    print("Shape:", df.shape)

    print("\nDtypes:")
    print(df.dtypes)

    print("\nMissing values:")
    print(df.isna().sum().sort_values(ascending=False))


def numeric_summary(df):
    print("\n=====NUMERIC SUMMARY=====")
    print(df.describe())


def plot_standard_value(df):
    if "standard_value" not in df.columns:
        return
    plt.figure()
    df["standard_value"].dropna().hist(bins=100)
    plt.xlabel("standard_value")
    plt.ylabel("Count")
    plt.title("Distribution of standard_value")
    plt.yscale("log")
    plt.show()

def plot_standard_valueLOG(df):
    if "standard_value" not in df.columns:
        return
    values = df["standard_value"].dropna()
    values = values[values > 0]
    log_values = np.log10(values)
    plt.figure()
    plt.hist(log_values, bins=100)
    plt.xlabel("log10(standard_value)")
    plt.ylabel("Count")
    plt.title("Distribution of log10(standard_value)")
    plt.show()


def plot_pchembl(df):
    if "pchembl_value" not in df.columns:
        return
    plt.figure()
    df["pchembl_value"].dropna().hist(bins=100)
    plt.xlabel("pchembl_value")
    plt.ylabel("Count")
    plt.title("Distribution of pchembl_value")
    plt.show()


def standard_type_counts(df):
    if "standard_type" in df.columns:
        print("\n=====STANDARD TYPE COUNTS=====")
        print(df["standard_type"].value_counts())


def correlation_analysis(df):
    print("\n=====CORRELATION MATRIX=====")
    exclude_cols = ["activity_id", "assay_id", "doc_id", "record_id", "molregno", "potential_duplicate", "toid", "standard_upper_value", "text_value", "standard_text_value"]
    numeric_df = (
        df
        .select_dtypes(include=[np.number])
        .drop(columns=exclude_cols, errors="ignore")
    )
    if numeric_df.shape[1] < 2:
        print("############ Not enough numeric columns for correlation.")
        return
    corr = numeric_df.corr()
    print(corr)
    plt.figure(figsize=(14, 13))
    plt.imshow(corr)
    plt.colorbar()
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=90)
    plt.yticks(range(len(corr.columns)), corr.columns)
    plt.title("Correlation Matrix")
    plt.show()


def correlation_analysis_full(df):
    print("\n=====CORRELATION MATRIX=====")
    numeric_df = df.select_dtypes(include=[np.number])
    if numeric_df.shape[1] < 2:
        print("############ Not enough numeric columns for correlation.")
        return
    corr = numeric_df.corr()
    print(corr)
    plt.figure(figsize=(14, 13))
    plt.imshow(corr)
    plt.colorbar()
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=90)
    plt.yticks(range(len(corr.columns)), corr.columns)
    plt.title("Correlation Matrix")
    plt.show()

def show_molecules(df, n=6):
    if "canonical_smiles" not in df.columns:
        print("No SMILES column found.")
        return
    print("\n=====MOLECULAR STRUCTURES=====")
    smiles_list = df["canonical_smiles"].dropna().unique()[:n]
    mols = []
    for smi in smiles_list:
        mol = Chem.MolFromSmiles(smi)
        if mol:
            mols.append(mol)
    if not mols:
        print("No valid molecules to display.")
        return
    img = Draw.MolsToGridImage(
        mols,
        molsPerRow=3,
        subImgSize=(250, 250)
    )
    img.show()

def relation_statistics(df):
    print("\n===== RELATION OPERATORS STATISTICS =====")
    relation_cols = ["standard_relation", "relation"]
    for col in relation_cols:
        if col not in df.columns:
            continue
        print(f"\nColumn: {col}")
        counts = df[col].value_counts(dropna=False)
        percentages = df[col].value_counts(normalize=True, dropna=False) * 100
        stats = (
            counts.to_frame(name="count")
            .join(percentages.to_frame(name="percentage"))
        )
        print(stats)


def main():
    df = pd.read_parquet(DATA_PATH)
    basic_info(df)
    numeric_summary(df)
    standard_type_counts(df)
    plot_standard_value(df)
    plot_standard_valueLOG(df)
    plot_pchembl(df)
    correlation_analysis_full(df)
    correlation_analysis(df)
    show_molecules(df, n=21)
    relation_statistics(df)

if __name__ == "__main__":
    main()