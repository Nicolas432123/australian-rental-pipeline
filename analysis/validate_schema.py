import argparse
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Validate schema of cleaned rentals data")
    parser.add_argument("--input", required=True, help="Path to cleaned parquet file")
    args = parser.parse_args()

    df = pd.read_parquet(args.input)

    print("\n===== SHAPE =====")
    print(df.shape)

    print("\n===== DTYPES =====")
    print(df.dtypes)

    print("\n===== NULLS =====")
    print(df.isnull().sum())

    print("\n===== SAMPLE =====")
    print(df.head())

    if df.empty or len(df.columns) == 0:
        raise ValueError("Input parquet is empty or has no columns.")

    print("\n===== DESCRIBE =====")
    print(df.describe(include="all"))


if __name__ == "__main__":
    main()