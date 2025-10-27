import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import numpy as np 

# --- Database Configuration ---
engine = create_engine("mysql://harsh:hva123@localhost/stock_data")

# --- Target Table Name ---
# Renamed target to reflect it uses weight breakdown data
TARGET_TABLE = 'index_weight_breakdown_usd_mcap' 

def normalize_market_cap_to_usd():
    """
    FIXED: Now queries the 'index_weight_breakdown' table which contains 'marketcap'.
    1. Joins index_weight_breakdown with currency_rates via country_currency_map.
    2. Calculates market_cap_usd by DIVIDING by rate_to_usd.
    3. Saves the final, normalized dataset to a new table.
    """
    print("Starting market cap normalization to USD... 💰")

    # --- 1. Define the SQL Query for the Join (Source table changed!) ---
    normalization_query = f"""
    SELECT
        iwb.index_name,
        iwb.country,
        iwb.sector,
        iwb.marketcap,         -- Explicitly selecting the marketcap column
        iwb.weight__pct,       -- Keeping the weight percentage
        ccm.currency_code,
        cr.rate_to_usd
    FROM
        index_weight_breakdown AS iwb  -- <--- THE SOURCE TABLE IS NOW CORRECT
    JOIN
        country_currency_map AS ccm ON iwb.country = ccm.country_name
    LEFT JOIN 
        currency_rates AS cr ON ccm.currency_code = cr.currency_code;
    """
    
    try:
        # --- 2. Read the joined data into a Pandas DataFrame ---
        df = pd.read_sql(text(normalization_query), con=engine)
        
        if df.empty:
            print("Query returned no data. Check if 'index_weight_breakdown' and currency tables have been populated.")
            return

        print(f"Successfully loaded {len(df)} records for processing.")
        
        # --- 3. Perform the Market Cap Conversion (Confirmed Column Name) ---
        
        # FIX: The column is definitively 'marketcap' from the join
        market_cap_col = 'marketcap' 

        print(f"Using original market cap column: '{market_cap_col}'")
        
        # Avoid division by zero by replacing 0s with NaN in the rate column first
        df['rate_to_usd'] = df['rate_to_usd'].replace(0, np.nan)
        
        # REVISED CALCULATION: DIVISION (Local Market Cap / Rate to USD)
        df['market_cap_usd'] = df[market_cap_col] / df['rate_to_usd']
        
        # --- 4. Clean up and select final columns ---
        final_cols = [
            'index_name',
            'country',
            'sector',
            'currency_code',
            market_cap_col,           
            'market_cap_usd',         
            'rate_to_usd',            
            'weight__pct'
        ]
        
        df_final = df[[col for col in final_cols if col in df.columns]]
        
        missing_rates = df_final['market_cap_usd'].isna().sum()
        
        if missing_rates > 0:
            print(f"Warning: {missing_rates} records resulted in missing/invalid USD Market Cap.")

        # --- 5. Save the result to the new SQL table ---
        print(f"Saving {len(df_final)} records to the database table '{TARGET_TABLE}'...")

        df_final.to_sql(
            TARGET_TABLE,
            con=engine,
            if_exists='replace', 
            index=False,
            chunksize=500,
            method="multi"
        )
        
        print(f"✅ Success! Data normalized and saved to '{TARGET_TABLE}'.")
        print("\nFirst 5 rows of the new normalized data:")
        print(df_final.head().to_string())
        
    except SQLAlchemyError as e:
        print(f"\nDatabase Error: An SQLAlchemy error occurred: {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    normalize_market_cap_to_usd()