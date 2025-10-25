import numpy as np
import pandas as pd
import os
import glob
from sqlalchemy import create_engine

# --- Database Configuration ---
engine = create_engine("mysql://harsh:hva123@localhost/stock_data")

# --- Stock Data Configuration ---
# !! Make sure this path is correct !!
DATA_DIRECTORY = r"/home/harshvardhan/dcv/Stock-Market-Indices/index_analysis_reports"

# 1. Find all CSV files
csv_files = glob.glob(os.path.join(DATA_DIRECTORY, "*.csv"))

if not csv_files:
    print(f"Warning: No CSV files were found in the directory: {DATA_DIRECTORY}")
else:
    print(f"Found {len(csv_files)} CSV files. Starting import...")

# 2. Process each CSV file
for file_path in csv_files:
    try:
        file_name = os.path.basename(file_path)
        base_name = os.path.splitext(file_name)[0]

        table_name = None
        full_name_from_file = None # Will be e.g., "ATHEX_Large_Cap_Greece"

        # Determine the table and full index name
        if base_name.endswith('_count_breakdown'):
            table_name = 'index_count_breakdown'
            full_name_from_file = base_name.removesuffix('_count_breakdown')
            
        elif base_name.endswith('_weight_breakdown'):
            table_name = 'index_weight_breakdown'
            full_name_from_file = base_name.removesuffix('_weight_breakdown')
        
        else:
            print(f"Skipping '{file_name}': does not match naming pattern.")
            continue

        # --- [NEW LOGIC] ---
        # Split "ATHEX_Large_Cap_Greece" into "ATHEX_Large_Cap" and "Greece"
        parts = full_name_from_file.rsplit('_', 1)
        
        index_name = None
        country = None
        
        if len(parts) == 2:
            index_name = parts[0] # e.g., "ATHEX_Large_Cap"
            country = parts[1]    # e.g., "Greece"
        else:
            # Fallback in case of an unexpected filename format
            index_name = full_name_from_file
            country = 'Unknown'
        
        print(f"Processing '{file_name}':")
        print(f"  -> Index: '{index_name}'")
        print(f"  -> Country: '{country}'")
        print(f"  -> Table: '{table_name}'")

        # Read the CSV data
        df = pd.read_csv(file_path)
        
        # [UPDATED] Clean the *original* columns from the CSV first
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(' ', '_')
            .str.replace(r'\(%\)', '_pct', regex=True)
            .str.replace(r'[^a-z0-a-zA-Z0-9_]', '', regex=True) # Fixed regex to allow numbers
        )
        
        # [NEW] Add the new, clean columns
        df['index_name'] = index_name
        df['country'] = country

        # [UPDATED] Re-order columns to put new ones first
        # Get a list of all columns *except* our new ones
        original_cols = [col for col in df.columns if col not in ['index_name', 'country']]
        
        # Create the new column order
        new_cols = ['index_name', 'country'] + original_cols
        df = df[new_cols]

        # Write the DataFrame to the shared SQL table
        # Since the tables were dropped, if_exists="append" will act like "create"
        # on the first run, and then append for all other files.
        df.to_sql(
            table_name,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi"
        )
        
        print(f"  -> Successfully appended data to table '{table_name}'.")

    except Exception as e:
        print(f"An error occurred while processing {file_path}: {e}")

print("\nAll stock data files have been processed.")