import pandas as pd
from sqlalchemy import create_engine

# --- Database Configuration ---
engine = create_engine("mysql://harsh:hva123@localhost/stock_data")

print("Connecting to the database...")

try:
    # --- 1. Get a list of all unique indices in the 'weight' table ---
    print("\n--- Unique Indices (from weight table) ---")
    
    query_indices = "SELECT DISTINCT index_name FROM index_weight_breakdown"
    df_indices = pd.read_sql(query_indices, con=engine)
    
    print(df_indices.to_string())

    # --- 2. Get the Top 5 SECTORS by weight for ONE index ---
    
    if not df_indices.empty:
        example_index = df_indices.iloc[0]['index_name']
        
        print(f"\n--- Top 5 Sectors by Weight for: '{example_index}' ---")
        
        # [FIXED] Changed 'weight_pct' to 'weight__pct' (double underscore)
        # to match the name created by your db_maker.py script.
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

    # --- 3. Get all data for one specific index (count breakdown) ---
    if not df_indices.empty:
        example_index = df_indices.iloc[0]['index_name'] 
        
        print(f"\n--- Full Count Breakdown for: '{example_index}' ---")
        
        query_full_count = f"""
            SELECT * FROM index_count_breakdown
            WHERE index_name = '{example_index}'
        """
        
        df_full_count = pd.read_sql(query_full_count, con=engine)
        print(df_full_count)

except Exception as e:
    print(f"An error occurred while reading from the database: {e}")

print("\nDatabase queries complete.")