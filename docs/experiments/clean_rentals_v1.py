import pandas as pd

INPUT_PATH = "data/raw/australian_rental_market_2026.csv"
OUTPUT_PATH = "data/processed/rentals_clean.csv"

df = pd.read_csv(INPUT_PATH)

print("Rows raw:", len(df))

# crear id
df["listing_id"] = range(1, len(df) + 1)

# eliminar columnas innecesarias
df = df.drop(columns=["title", "locality"])

# renombrar
df = df.rename(columns={
    "price_display": "price_aud",
    "description": "listing_description",
    "propertyType": "property_type"
})

# rellenar amenities
df["amenities"] = df["amenities"].fillna("unknown")

# eliminar sin precio
df = df[df["price_aud"].notnull()]

# eliminar duplicados simples
df = df.drop_duplicates()

print("Rows clean:", len(df))

df.to_csv(OUTPUT_PATH, index=False)

print("Clean dataset saved.")