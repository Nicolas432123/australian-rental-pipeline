import pandas as pd

file_path = "data/raw/australian_rental_market_2026.csv"

df = pd.read_csv(file_path)

print("\nColumns:")
print(df.columns)

print("\nNumber of rows:")
print(len(df))

print("\nSample data:")
print(df.head())