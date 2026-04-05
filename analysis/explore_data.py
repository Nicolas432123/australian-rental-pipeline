import pandas as pd

df = pd.read_csv("data/raw/australian_rental_market_2026.csv")

print("\nProperty types:")
print(df["propertyType"].value_counts())

print("\nBedrooms stats:")
print(df["bedrooms"].describe())

print("\nParking stats:")
print(df["parking_spaces"].describe())

print("\nPrice stats:")
print(df["price_display"].describe())