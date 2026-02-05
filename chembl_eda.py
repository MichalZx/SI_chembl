import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

DATA_PATH = Path("Dane/chembl_activity_cleaned_00.parquet")

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
    numeric_df = df.select_dtypes(include=[np.number])

    if numeric_df.shape[1] < 2:
        print("############ Not enough numeric columns for correlation.")
        return

    corr = numeric_df.corr()
    print(corr)

    plt.figure()
    plt.imshow(corr)
    plt.colorbar()
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=90)
    plt.yticks(range(len(corr.columns)), corr.columns)
    plt.title("Correlation Matrix")
    plt.show()


if __name__ == "__main__":
    df = pd.read_parquet(DATA_PATH)
    basic_info(df)
    numeric_summary(df)
    standard_type_counts(df)
    plot_standard_value(df)
    plot_pchembl(df)
    correlation_analysis(df)