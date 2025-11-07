import pandas as pd

# 1. Define your file paths
parquet_file_path = '/home/harshvardhan/dcv4/Stock-Market-Indices/stock-market-indices-v2/data/final/final_all_historical_prices_parquet.parquet'
csv_file_path = '/home/harshvardhan/dcv4/Stock-Market-Indices/stock-market-indices-v2/data/final/final_all_historical_prices.csv'
try:
    # 2. Read the Parquet file into a pandas DataFrame
    print(f"Reading {parquet_file_path}...")
    df = pd.read_parquet(parquet_file_path)

    # 3. Write the DataFrame to a CSV file
    # index=False prevents pandas from writing the DataFrame index as a column
    print(f"Writing {csv_file_path}...")
    df.to_csv(csv_file_path, index=False)

    print("Conversion complete!")

except FileNotFoundError:
    print(f"Error: The file '{parquet_file_path}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")