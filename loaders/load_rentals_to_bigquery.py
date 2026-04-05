import argparse
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery


def load_parquet_folder(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path, engine="pyarrow")

    if df.empty:
        raise ValueError("Loaded dataframe is empty.")

    return df


def standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = [
        "listing_id",
        "price_aud",
        "property_type",
        "suburb",
        "state",
        "postcode",
        "latitude",
        "longitude",
        "bedrooms",
        "bathrooms",
        "parking_spaces",
        "agency_name",
        "amenities",
        "listing_description",
    ]

    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")

    df = df[expected_columns].copy()
    df["loaded_at"] = datetime.now(timezone.utc)

    df["listing_id"] = df["listing_id"].astype("int64")
    df["price_aud"] = df["price_aud"].astype("int64")
    df["property_type"] = df["property_type"].astype("string")
    df["suburb"] = df["suburb"].astype("string")
    df["state"] = df["state"].astype("string")
    df["postcode"] = df["postcode"].astype("string")
    df["latitude"] = df["latitude"].astype("float64")
    df["longitude"] = df["longitude"].astype("float64")
    df["bedrooms"] = df["bedrooms"].astype("int64")
    df["bathrooms"] = df["bathrooms"].astype("int64")
    df["parking_spaces"] = df["parking_spaces"].astype("int64")
    df["agency_name"] = df["agency_name"].astype("string")
    df["amenities"] = df["amenities"].astype("string")
    df["listing_description"] = df["listing_description"].astype("string")

    return df


def ensure_dataset_exists(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    location: str,
) -> None:
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset already exists: {dataset_ref}")
    except Exception:
        client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset: {dataset_ref}")


def load_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    table_id: str,
    location: str,
) -> None:
    client = bigquery.Client(project=project_id)

    ensure_dataset_exists(client, project_id, dataset_id, location)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("listing_id", "INTEGER"),
            bigquery.SchemaField("price_aud", "INTEGER"),
            bigquery.SchemaField("property_type", "STRING"),
            bigquery.SchemaField("suburb", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("postcode", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("bedrooms", "INTEGER"),
            bigquery.SchemaField("bathrooms", "INTEGER"),
            bigquery.SchemaField("parking_spaces", "INTEGER"),
            bigquery.SchemaField("agency_name", "STRING"),
            bigquery.SchemaField("amenities", "STRING"),
            bigquery.SchemaField("listing_description", "STRING"),
            bigquery.SchemaField("loaded_at", "TIMESTAMP"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_ref}")


def main():
    parser = argparse.ArgumentParser(description="Load cleaned rentals parquet into BigQuery")
    parser.add_argument("--input", required=True, help="Path to cleaned parquet folder")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset ID")
    parser.add_argument("--table", required=True, help="BigQuery table ID")
    parser.add_argument(
        "--location",
        default="australia-southeast1",
        help="BigQuery dataset location",
    )
    args = parser.parse_args()

    parquet_path = args.input

    print("Reading parquet...")
    df = load_parquet_folder(parquet_path)
    print(f"Rows read from parquet: {len(df)}")

    print("Standardizing dataframe...")
    df = standardize_dataframe(df)
    print("Dataframe standardized successfully.")

    print("Loading to BigQuery...")
    load_to_bigquery(
        df=df,
        project_id=args.project,
        dataset_id=args.dataset,
        table_id=args.table,
        location=args.location,
    )

    print("Done.")


if __name__ == "__main__":
    main()