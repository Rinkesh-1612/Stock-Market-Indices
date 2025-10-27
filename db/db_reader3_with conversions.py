import pandas as pd
from sqlalchemy import create_engine

# --- Database Configuration ---
engine = create_engine("mysql://harsh:hva123@localhost/stock_data")

print("Connecting to the database...")

try:
    # ------------------------------------------------------------------
    # --- EXISTING QUERIES (Original Script) ---
    # ------------------------------------------------------------------
    
    # --- 1. Get a list of all unique indices in the 'weight' table ---
    print("\n--- Unique Indices (from weight table) ---")
    
    query_indices = "SELECT DISTINCT index_name FROM index_weight_breakdown"
    df_indices = pd.read_sql(query_indices, con=engine)
    
    print(df_indices.to_string())

    # ------------------------------------------------------------------
    
    # --- 2. Get the Top 5 SECTORS by weight for ONE index ---
    
    if not df_indices.empty:
        example_index = df_indices.iloc[0]['index_name']
        
        print(f"\n--- Top 5 Sectors by Weight for: '{example_index}' ---")
        
        query_top5 = f"""
            SELECT sector, weight__pct 
            FROM index_weight_breakdown
            WHERE index_name = '{example_index}'
            ORDER BY weight__pct DESC
            LIMIT 5
        """
        
        df_top5 = pd.read_sql(query_top5, con=engine)
        print(df_top5.to_string())
    
    else:
        print("\nCould not find any indices in 'index_weight_breakdown' to query.")

    # ------------------------------------------------------------------
    
    # --- 3. Get all data for one specific index (count breakdown) ---
    if not df_indices.empty:
        # Example index is already defined from step 1
        
        print(f"\n--- Full Count Breakdown for: '{example_index}' ---")
        
        query_full_count = f"""
            SELECT * FROM index_count_breakdown
            WHERE index_name = '{example_index}'
        """
        
        df_full_count = pd.read_sql(query_full_count, con=engine)
        print(df_full_count)
        
    # ------------------------------------------------------------------
    # --- NEW QUERIES (New Tables) ---
    # ------------------------------------------------------------------
    
    # --- 4. Get the full Country-to-Currency Map ---
    print("\n" + "="*50)
    print("--- 4. Country to Currency Mapping Table ---")
    print("="*50)
    
    query_map = "SELECT * FROM country_currency_map ORDER BY country_name"
    df_map = pd.read_sql(query_map, con=engine)
    
    # Print a summary or the full table
    if not df_map.empty:
        print(f"Total mappings found: {len(df_map)}")
        print("\nFirst 5 rows of Country-Currency Map:")
        print(df_map.head().to_string())
    else:
        print("Table 'country_currency_map' is empty.")


    # --- 5. Get the full Currency Rates Table ---
    print("\n" + "="*50)
    print("--- 5. Currency Rates (USD Conversion) Table ---")
    print("="*50)
    
    query_rates = "SELECT * FROM currency_rates ORDER BY currency_code"
    df_rates = pd.read_sql(query_rates, con=engine)
    
    # Print the full table
    if not df_rates.empty:
        print(f"Total currency rates found: {len(df_rates)}")
        print("\nFull Currency Rates Table:")
        print(df_rates.to_string())
    else:
        print("Table 'currency_rates' is empty.")
        
    # ------------------------------------------------------------------
    # --- NEWEST QUERY (Demonstrating Normalization) ---
    # ------------------------------------------------------------------
    
    # --- 6. Get the Bottom 6 Normalized Market Cap Records ---
    print("\n" + "="*50)
    print("--- 6. BOTTOM 6 Normalized Market Cap (USD) Records ---")
    print("Sorted Ascending by market_cap_usd")
    print("="*50)
    
    query_normalized = f"""
        SELECT 
            index_name, 
            country, 
            currency_code, 
            marketcap, 
            market_cap_usd, 
            weight__pct
        FROM 
            index_weight_breakdown_usd_mcap 
        ORDER BY 
            market_cap_usd ASC 
        LIMIT 6
    """
    df_normalized = pd.read_sql(query_normalized, con=engine)
    
    # Print the result
    if not df_normalized.empty:
        print(f"Successfully loaded {len(df_normalized)} rows.")
        print("\nRecords with the lowest Market Cap (USD):")
        print(df_normalized.to_string())
    else:
        print("Table 'index_weight_breakdown_usd_mcap' is empty. Run the normalizer script first.")


except Exception as e:
    print(f"An error occurred while reading from the database: {e}")

print("\nDatabase queries complete.")