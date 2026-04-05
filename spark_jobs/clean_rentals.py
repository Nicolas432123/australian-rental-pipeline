import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


VALID_PROPERTY_TYPES = [
    "house",
    "apartment",
    "unit",
    "townhouse",
    "duplex/semi-detached",
    "studio",
    "flat",
    "villa",
    "terrace",
]


def main():
    parser = argparse.ArgumentParser(
        description="Clean Australian rental listings with PySpark"
    )
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=True, help="Path to output Parquet directory")
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output

    spark = (
    SparkSession.builder
    .appName("clean_australian_rentals")
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/app/credentials/gcp-rentals-sa.json")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .getOrCreate()
)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    print("========== RAW DATA ==========")
    print(f"Raw rows: {df.count()}")
    df.printSchema()

    df_selected = df.select(
        F.col("price_display").alias("price_aud"),
        F.col("propertyType").alias("property_type"),
        F.col("suburb"),
        F.col("state"),
        F.col("postcode"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("bedrooms"),
        F.col("bathrooms"),
        F.col("parking_spaces"),
        F.col("agency_name"),
        F.col("amenities"),
        F.col("description").alias("listing_description")
    )

    # Normalización base de textos
    df_clean = (
        df_selected
        .withColumn("property_type", F.lower(F.trim(F.col("property_type"))))
        .withColumn("suburb", F.initcap(F.trim(F.col("suburb"))))
        .withColumn("state", F.upper(F.trim(F.col("state"))))
        .withColumn("agency_name", F.initcap(F.trim(F.col("agency_name"))))
        .withColumn("amenities", F.trim(F.col("amenities")))
        .withColumn("listing_description", F.trim(F.col("listing_description")))
    )

    # Estandarizar nombres de estados
    df_clean = (
        df_clean
        .withColumn(
            "state",
            F.when(F.col("state") == "VIC", "VIC")
             .when(F.col("state") == "QLD", "QLD")
             .when(F.col("state") == "NSW", "NSW")
             .when(F.col("state") == "WA", "WA")
             .when(F.col("state") == "SA", "SA")
             .when(F.col("state") == "TAS", "TAS")
             .when(F.col("state") == "NT", "NT")
             .when(F.col("state") == "ACT", "ACT")
             .otherwise(F.col("state"))
        )
    )

    # Rellenar amenities nulo o vacío con unknown
    df_clean = df_clean.withColumn(
        "amenities",
        F.when(
            F.col("amenities").isNull() | (F.col("amenities") == ""),
            F.lit("unknown")
        ).otherwise(F.col("amenities"))
    )

    # Parseo y cast de columnas numéricas
    df_clean = df_clean.withColumn(
        "price_aud",
        F.regexp_extract(F.col("price_aud"), r"(\d+)", 1).cast("int")
    )

    df_clean = df_clean.withColumn("bedrooms", F.col("bedrooms").cast("int"))
    df_clean = df_clean.withColumn("bathrooms", F.col("bathrooms").cast("int"))
    df_clean = df_clean.withColumn("parking_spaces", F.col("parking_spaces").cast("int"))
    df_clean = df_clean.withColumn("latitude", F.col("latitude").cast("double"))
    df_clean = df_clean.withColumn("longitude", F.col("longitude").cast("double"))

    # Filtros de negocio
    df_clean = df_clean.filter(F.col("property_type").isin(VALID_PROPERTY_TYPES))
    df_clean = df_clean.filter(F.col("bedrooms") <= 8)
    df_clean = df_clean.filter(F.col("parking_spaces") <= 5)

    # Campos obligatorios
    df_clean = df_clean.filter(F.col("price_aud").isNotNull())
    df_clean = df_clean.filter(F.col("property_type").isNotNull())
    df_clean = df_clean.filter(F.col("suburb").isNotNull())
    df_clean = df_clean.filter(F.col("state").isNotNull())

    # Capitalización más amigable para property_type
    df_clean = df_clean.withColumn(
        "property_type",
        F.initcap(F.col("property_type"))
    )

    # listing_id ordenado
    window_spec = Window.orderBy(
        F.col("state"),
        F.col("suburb"),
        F.col("property_type"),
        F.col("price_aud")
    )

    df_clean = df_clean.withColumn(
        "listing_id",
        F.row_number().over(window_spec)
    )

    df_final = df_clean.select(
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
        "listing_description"
    )

    print("========== CLEAN DATA ==========")
    print(f"Clean rows: {df_final.count()}")
    df_final.printSchema()

    print("========== PROPERTY TYPE DISTRIBUTION ==========")
    df_final.groupBy("property_type").count().orderBy(F.col("count").desc()).show()

    (
        df_final.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Clean parquet written to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()