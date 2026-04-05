import pandas as pd

INPUT_PATH = "data/raw/australian_rental_market_2026.csv"
OUTPUT_PATH = "data/processed/rentals_clean_v2.csv"

df = pd.read_csv(INPUT_PATH)

print("Rows raw:", len(df))

# ID
df["listing_id"] = range(1, len(df) + 1)

# eliminar columnas
df = df.drop(columns=["title", "locality"])

# renombrar
df = df.rename(columns={
    "price_display": "price_aud",
    "description": "listing_description",
    "propertyType": "property_type"
})

# amenities
df["amenities"] = df["amenities"].fillna("unknown")

# filtro property type
valid_types = [
    "house",
    "apartment",
    "unit",
    "townhouse",
    "duplex/semi-detached",
    "studio",
    "flat",
    "villa",
    "terrace"
]

df = df[df["property_type"].isin(valid_types)]

# filtro outliers
df = df[df["bedrooms"] <= 8]
df = df[df["parking_spaces"] <= 5]

print("Rows after semantic cleaning:", len(df))

df = df.drop_duplicates()

df.to_csv(OUTPUT_PATH, index=False)

print("Clean v2 saved.")