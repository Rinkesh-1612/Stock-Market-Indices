import pandas as pd
import yfinance as yf
import time
import os
import re
from tqdm import tqdm
import logging

# --- Configuration (Unchanged) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data')
MASTER_INDEX_LIST_CSV = os.path.join(DATA_DIR, "master_indices_list.csv")
RAW_CONSTITUENTS_DIR = os.path.join(DATA_DIR, "constituent_lists")
FINAL_CONSTITUENTS_MASTER_FILE = os.path.join(DATA_DIR, "master_constituents_list.csv")
VERIFICATION_SUMMARY_FILE = os.path.join(DATA_DIR, "constituent_verification_summary.csv")

# --- Ticker Verification Engine (UPGRADED) ---
TICKER_VALIDATION_CACHE = {}

def is_valid_ticker(ticker):
    """Checks if a ticker is valid, using a cache to avoid repeated API calls."""
    if ticker in TICKER_VALIDATION_CACHE:
        return TICKER_VALIDATION_CACHE[ticker] is not None
    try:
        info = yf.Ticker(ticker).info
        if info.get('quoteType') and (info.get('marketCap') is not None or info.get('totalAssets') is not None):
            TICKER_VALIDATION_CACHE[ticker] = ticker
            return True
    except Exception:
        pass
    TICKER_VALIDATION_CACHE[ticker] = None
    return False

def find_valid_yfinance_ticker(row):
    """
    Takes a row and intelligently parses multiple possible columns to find a valid yfinance ticker.
    """
    # Create a list of potential raw ticker strings from the row
    potential_raw_tickers = []
    for col in ['Cleaned Ticker', 'Raw Ticker', 'Company Ticker']:
        if col in row and pd.notna(row[col]):
            potential_raw_tickers.append(str(row[col]))

    # --- Strategy 1: Parse the raw strings to find a valid ticker ---
    for raw_string in potential_raw_tickers:
        # 1. Split by semicolon for cases like "GOOG; GOOGL"
        candidates = raw_string.split(';')
        for cand in candidates:
            # 2. Split by colon for "NYSE: MMM" and take the last part
            ticker_part = cand.strip().split(':')[-1]
            # 3. Clean up whitespace and take the first "word"
            cleaned_ticker = ticker_part.strip().split(' ')[0]
            
            if cleaned_ticker and is_valid_ticker(cleaned_ticker):
                return cleaned_ticker # Return the first valid one we find

    # --- Strategy 2: Fallback to searching by Company Name ---
    if 'Company Name' in row and pd.notna(row['Company Name']):
        company_name = str(row['Company Name']).strip()
        if company_name in TICKER_VALIDATION_CACHE and TICKER_VALIDATION_CACHE[company_name] is not None:
             return TICKER_VALIDATION_CACHE[company_name]
        try:
            search_results = yf.search(company_name)
            if not search_results.empty:
                found_ticker = search_results.index[0]
                if is_valid_ticker(found_ticker):
                    TICKER_VALIDATION_CACHE[company_name] = found_ticker
                    return found_ticker
        except: pass
        TICKER_VALIDATION_CACHE[company_name] = None

    return None

def main():
    try:
        master_list = pd.read_csv(MASTER_INDEX_LIST_CSV)
    except FileNotFoundError:
        logging.error(f"FATAL: Master index list not found at '{MASTER_INDEX_LIST_CSV}'. Please run script 1 first.")
        return

    all_verified_constituents = []
    verification_summary = []
    
    logging.info("--- Starting Phase 2b: Normalizing and Verifying All Constituents (v2) ---")

    for _, index_row in master_list.iterrows():
        index_name, index_ticker = index_row['Index Name'], index_row['Ticker']
        
        print("\n" + "="*70)
        logging.info(f"Processing Index: {index_name} ({index_ticker})")
        
        safe_ticker_name = re.sub(r'[\^.:]', '_', index_ticker)
        raw_filepath = os.path.join(RAW_CONSTITUENTS_DIR, f"{safe_ticker_name}.csv")
        
        if not os.path.exists(raw_filepath):
            logging.warning(f"  - Raw constituent file not found. Skipping.")
            verification_summary.append({"Index Name": index_name, "Raw Count": 0, "Verified Count": 0, "Success Rate": 0})
            continue

        df_raw = pd.read_csv(raw_filepath)
        raw_count = len(df_raw)
        if raw_count == 0:
            logging.info("  - Raw file is empty. Skipping.")
            verification_summary.append({"Index Name": index_name, "Raw Count": 0, "Verified Count": 0, "Success Rate": 0})
            continue

        verified_rows = []
        for _, row in tqdm(df_raw.iterrows(), total=raw_count, desc="  Verifying"):
            # Pass the entire row to the smart function
            valid_ticker = find_valid_yfinance_ticker(row)
            
            if valid_ticker:
                verified_rows.append({
                    "Index Name": index_name,
                    "Company Name": row.get('Company Name', 'N/A'),
                    "Company Ticker": valid_ticker
                })
        
        if verified_rows:
            df_verified = pd.DataFrame(verified_rows).drop_duplicates()
            all_verified_constituents.append(df_verified)
            verified_count = len(df_verified)
            success_rate = (verified_count / raw_count) * 100
            logging.info(f"  ✅ Verification complete. Found {verified_count}/{raw_count} valid tickers ({success_rate:.1f}% success).")
        else:
            verified_count = 0
            success_rate = 0
            logging.error(f"  - Verification failed. Found 0 valid tickers out of {raw_count}.")

        verification_summary.append({"Index Name": index_name, "Raw Count": raw_count, "Verified Count": verified_count, "Success Rate": f"{success_rate:.1f}%"})
        
        time.sleep(0.5)

    logging.info("\n\n" + "="*80)
    logging.info("--- FINAL VERIFICATION SUMMARY ---")
    
    if all_verified_constituents:
        master_df = pd.concat(all_verified_constituents, ignore_index=True)
        master_df.to_csv(FINAL_CONSTITUENTS_MASTER_FILE, index=False)
        logging.info(f"Successfully created final master constituent file with {len(master_df)} total verified entries.")
        logging.info(f"File saved to: '{os.path.abspath(FINAL_CONSTITUENTS_MASTER_FILE)}'")
    else:
        logging.warning("No constituents could be verified. Master file not created.")
        
    report_df = pd.DataFrame(verification_summary)
    report_df = report_df.sort_values(by="Verified Count", ascending=False)
    print(report_df.to_string())
    report_df.to_csv(VERIFICATION_SUMMARY_FILE, index=False)
    logging.info(f"\n✅ Full verification summary saved to '{os.path.abspath(VERIFICATION_SUMMARY_FILE)}'")


if __name__ == "__main__":
    main()