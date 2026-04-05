import argparse
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Validate business rules for rentals data")
    parser.add_argument("--input", required=True, help="Path to cleaned parquet file")
    args = parser.parse_args()

    df = pd.read_parquet(args.input)

    print("\n===== PRICE NEGATIVE =====")
    print(df[df["price_aud"] <= 0].shape)

    print("\n===== BEDROOMS > 8 =====")
    print(df[df["bedrooms"] > 8].shape)

    print("\n===== PARKING > 5 =====")
    print(df[df["parking_spaces"] > 5].shape)

    print("\n===== PROPERTY TYPE UNIQUE =====")
    print(df["property_type"].unique())

    print("\n===== STATES =====")
    print(df["state"].value_counts())

    print("\n===== COORDINATES NULL =====")
    print(df[df["latitude"].isnull() | df["longitude"].isnull()].shape)

    # 🔥 VALIDACIÓN REAL (esto es clave para pipeline)
    if (df["price_aud"] <= 0).any():
        raise ValueError("Found negative or zero prices")

    if (df["bedrooms"] > 8).any():
        raise ValueError("Found unrealistic number of bedrooms")

    if (df["parking_spaces"] > 5).any():
        raise ValueError("Found unrealistic number of parking spaces")

    if df["latitude"].isnull().any() or df["longitude"].isnull().any():
        raise ValueError("Found null coordinates")


if __name__ == "__main__":
    main()