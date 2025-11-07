import pandas as pd
import yfinance as yf
import time
import os
from tqdm import tqdm
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data')

# --- Inputs ---
MASTER_INDEX_LIST_CSV = os.path.join(DATA_DIR, "master_indices_list.csv")
MASTER_CONSTITUENTS_CSV = os.path.join(DATA_DIR, "master_constituents_list.csv")
VERIFICATION_SUMMARY_CSV = os.path.join(DATA_DIR, "constituent_verification_summary.csv")

# --- Outputs ---
FINAL_COMPANY_INFO_CSV = os.path.join(DATA_DIR, "final_company_info.csv")
FINAL_ALL_PRICES_CSV = os.path.join(DATA_DIR, "final_all_historical_prices.csv")

# --- Quality Control Threshold ---
MIN_SUCCESS_RATE_PERCENT = 50.0

def fetch_company_info(tickers_to_fetch):
    """Fetches detailed info (Sector, MarketCap, etc.) for a given list of tickers."""
    logging.info(f"--- Starting Company Info Fetch for {len(tickers_to_fetch)} unique companies ---")
    logging.warning("This is a slow process and will take a significant amount of time.")
    
    all_info_data = []
    for ticker_symbol in tqdm(tickers_to_fetch, desc="Fetching Company Info"):
        try:
            info = yf.Ticker(ticker_symbol).info
            # We need at least a sector and market cap to be useful
            if info.get('sector') and info.get('marketCap'):
                all_info_data.append({
                    "Company Ticker": ticker_symbol,
                    "Sector": info['sector'],
                    "Industry": info.get('industry', 'N/A'),
                    "MarketCap": info['marketCap'],
                    "Currency": info.get('currency', 'N/A')
                })
            time.sleep(0.05) # Be polite to the API
        except Exception:
            # This is expected for some tickers, so we just log a warning for every 100th failure
            if len(all_info_data) % 100 == 0:
                logging.warning(f"Could not fetch info for {ticker_symbol}. Skipping.")
            continue
            
    if not all_info_data:
        logging.error("No company info could be fetched.")
        return pd.DataFrame()

    df = pd.DataFrame(all_info_data)
    logging.info(f"Successfully fetched info for {len(df)} companies.")
    return df

def fetch_historical_prices(tickers_to_fetch, period="10y", interval="1d"):
    """Fetches historical closing prices for a list of tickers in a single batch call."""
    logging.info(f"--- Starting Historical Price Fetch for {len(tickers_to_fetch)} total tickers ({period}, {interval}) ---")
    
    # yfinance can handle large lists, but it's safer to batch them
    batch_size = 400
    all_price_data = []
    
    for i in tqdm(range(0, len(tickers_to_fetch), batch_size), desc="Fetching Price Batches"):
        batch = tickers_to_fetch[i:i+batch_size]
        try:
            data = yf.download(batch, period=period, interval=interval, auto_adjust=True, progress=False)
            if not data.empty:
                all_price_data.append(data['Close'])
        except Exception as e:
            logging.error(f"Error fetching batch starting with {batch[0]}: {e}")
        time.sleep(1) # Pause between large batches

    if not all_price_data:
        logging.error("No historical price data could be fetched.")
        return pd.DataFrame()
        
    final_prices_df = pd.concat(all_price_data, axis=1)
    # yfinance can return duplicate columns if a ticker is in multiple batches; remove them
    final_prices_df = final_prices_df.loc[:, ~final_prices_df.columns.duplicated()]
    
    logging.info(f"Successfully fetched price data for {len(final_prices_df.columns)} unique tickers.")
    return final_prices_df

def main():
    try:
        df_master_indices = pd.read_csv(MASTER_INDEX_LIST_CSV)
        df_master_constituents = pd.read_csv(MASTER_CONSTITUENTS_CSV)
        df_verification = pd.read_csv(VERIFICATION_SUMMARY_CSV)
    except FileNotFoundError as e:
        logging.error(f"FATAL: A required input file is missing: {e}. Please run previous scripts.")
        return

    # --- Step 1: Identify the "High-Quality" Indices based on your rule ---
    df_verification['Success Rate'] = df_verification['Success Rate'].astype(str).str.replace('%', '').astype(float)
    high_quality_indices = df_verification[df_verification['Success Rate'] >= MIN_SUCCESS_RATE_PERCENT]['Index Name'].tolist()
    
    logging.info(f"Identified {len(high_quality_indices)} indices with >= {MIN_SUCCESS_RATE_PERCENT}% constituent verification rate.")
    
    # Filter the master constituent list to only include these high-quality indices
    df_high_quality_constituents = df_master_constituents[df_master_constituents['Index Name'].isin(high_quality_indices)]
    
    # --- Step 2: Create the unique lists of tickers we need to fetch data for ---
    # List A: All company tickers from the high-quality indices
    company_tickers_to_fetch = sorted(list(df_high_quality_constituents['Company Ticker'].unique()))
    
    # List B: All 71 original index tickers
    index_tickers_to_fetch = sorted(list(df_master_indices['Ticker'].unique()))
    
    # Combine them into one grand list for the price fetch
    all_tickers_for_prices = sorted(list(set(company_tickers_to_fetch + index_tickers_to_fetch)))
    
    logging.info(f"Plan: Fetching company info for {len(company_tickers_to_fetch)} companies.")
    logging.info(f"Plan: Fetching historical prices for {len(all_tickers_for_prices)} total unique tickers.")

    # --- Step 3: Execute the data fetching ---
    df_company_info = fetch_company_info(company_tickers_to_fetch)
    df_all_prices = fetch_historical_prices(all_tickers_for_prices)
    
    # --- Step 4: Save the final, clean datasets ---
    if not df_company_info.empty:
        df_company_info.to_csv(FINAL_COMPANY_INFO_CSV, index=False)
        logging.info(f"\n✅ Final Company Info file saved to: {os.path.abspath(FINAL_COMPANY_INFO_CSV)}")
    else:
        logging.warning("Company info file was not saved as no data was fetched.")

    if not df_all_prices.empty:
        df_all_prices.to_csv(FINAL_ALL_PRICES_CSV)
        logging.info(f"✅ Final Historical Prices file saved to: {os.path.abspath(FINAL_ALL_PRICES_CSV)}")
    else:
        logging.warning("Historical prices file was not saved as no data was fetched.")
        
    logging.info("\n--- Phase 3 Data Fetching Complete ---")


if __name__ == "__main__":
    main()