import argparse
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Validate data volume between raw and clean datasets")
    parser.add_argument("--raw", required=True, help="Path to raw CSV file")
    parser.add_argument("--clean", required=True, help="Path to cleaned parquet file")
    parser.add_argument("--max-removal-pct", type=float, default=20.0, help="Max % of rows allowed to be removed")
    args = parser.parse_args()

    raw = pd.read_csv(args.raw)
    clean = pd.read_parquet(args.clean)

    raw_rows = len(raw)
    clean_rows = len(clean)
    removed = raw_rows - clean_rows
    pct_removed = (removed / raw_rows) * 100

    print("\n===== RAW ROWS =====")
    print(raw_rows)

    print("\n===== CLEAN ROWS =====")
    print(clean_rows)

    print("\n===== ROWS REMOVED =====")
    print(removed)

    print("\n===== % REMOVED =====")
    print(round(pct_removed, 2))

    if clean_rows == 0:
        raise ValueError("Clean dataset is empty")

    if pct_removed > args.max_removal_pct:
        raise ValueError(f"Too many rows removed: {pct_removed:.2f}% (threshold: {args.max_removal_pct}%)")


if __name__ == "__main__":
    main()