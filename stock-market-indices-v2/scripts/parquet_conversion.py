import pandas as pd

# --- Configuration ---
csv_file_path = r'D:\Stock-Market-Indices\stock-market-indices-v2\data\final\final_all_historical_prices.csv'
parquet_file_path = 'final_all_historical_prices_parquet.parquet'
# ---------------------

print(f"Reading {csv_file_path}...")
try:
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file_path)

    print(f"Writing {parquet_file_path}...")
    # Write the DataFrame to a Parquet file
    # The 'engine='pyarrow'' is technically the default if you have it installed,
    # but it's good practice to be explicit.
    df.to_parquet(parquet_file_path, engine='pyarrow', index=False)

    print("Conversion successful!")
    print(f"DataFrame shape: {df.shape}")

except FileNotFoundError:
    print(f"Error: The file '{csv_file_path}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")