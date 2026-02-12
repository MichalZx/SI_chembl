import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

DATA_PATH = Path("Dane/chembl_ml_dataset_00.parquet")

def plot_descriptor_distributions(df):
    descriptors = ["MolWt", "LogP", "TPSA", "HBD", "HBA"]
    for col in descriptors:
        if col in df.columns:
            plt.figure()
            plt.hist(df[col].dropna(), bins=80)
            plt.xlabel(col)
            plt.ylabel("Count")
            plt.title(f"Distribution of {col}")
            plt.show()

        if col in df.columns:
            values = df[col].dropna()
            values = values[values > 0]
            if len(values) == 0:
                continue
            log_values = np.log10(values)
            plt.figure()
            plt.hist(log_values, bins=80)
            plt.xlabel(f"log10({col})")
            plt.ylabel("Count")
            plt.title(f"Distribution of log10({col})")
            plt.show()


def plot_logp_vs_activity(df):
    if "LogP" not in df.columns or "pIC50" not in df.columns:
        return
    plt.figure()
    plt.scatter(df["LogP"], df["pIC50"], alpha=0.3)
    plt.xlabel("LogP")
    plt.ylabel("pIC50")
    plt.title("LogP vs pIC50")
    plt.show()


def plot_mw_vs_activity(df):
    if "MolWt" not in df.columns or "pIC50" not in df.columns:
        return
    plt.figure()
    plt.scatter(df["MolWt"], df["pIC50"], alpha=0.3)
    plt.xlabel("Molecular Weight")
    plt.ylabel("pIC50")
    plt.title("Molecular Weight vs pIC50")
    plt.show()


def plot_correlation(df):
    numeric_df = df.select_dtypes(include=[np.number])
    corr = numeric_df.corr()
    plt.figure(figsize=(14, 13))
    plt.imshow(corr)
    plt.colorbar()
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=90)
    plt.yticks(range(len(corr.columns)), corr.columns)
    plt.title("Correlation Matrix")
    plt.show()


def main():
    df = pd.read_parquet(DATA_PATH)
    plot_descriptor_distributions(df)
    plot_logp_vs_activity(df)
    plot_mw_vs_activity(df)
    plot_correlation(df)

if __name__ == "__main__":
    main()