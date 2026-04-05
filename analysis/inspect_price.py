import pandas as pd

df = pd.read_csv("data/raw/australian_rental_market_2026.csv")

print(df["price_display"].head(20))
print(df["price_display"].dtype)