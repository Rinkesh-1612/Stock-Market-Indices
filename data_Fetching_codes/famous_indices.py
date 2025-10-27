import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
import logging
import io

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
HISTORICAL_DATA_PERIOD = "5y"
SLEEP_TIMER = 1.0 # Slightly reduced timer
OUTPUT_DIR = "index_market_data"

# --- Advanced, Configuration-Driven Ticker Scraping Engine (VERSION 5 - ROBUST) ---
INDEX_CONFIG = {

    # --- FIXES APPLIED BELOW ---
    "nikkei225": {"name": "Nikkei 225", "url": "https://en.wikipedia.org/wiki/Nikkei_225", "table_class": "wikitable", "ticker_column": "Symbol", "clean_fn": lambda s: f"{s.split(': ')[-1]}.T"},
    "hangseng": {
        "name": "Hang Seng",
        "url": "https://en.wikipedia.org/wiki/Hang_Seng_Index",
        "table_class": "wikitable",
        "ticker_column": "Ticker",
        # IMPROVED: This function now handles formats like "5" and "SEHK: 5"
        "clean_fn": lambda s: f"{int(s.split(':')[-1].strip()):04d}.HK"
    }
}

def get_tickers_from_wikipedia_robust(config):
    """
    IMPROVED: This function now iterates through all tables on a page and selects
    the first one that contains the required ticker column. This makes it
    resilient to page structure changes (like the Nikkei 225 issue).
    """
    name, url, table_class, ticker_col = config['name'], config['url'], config['table_class'], config['ticker_column']
    logging.info(f"Robustly scraping {name} tickers from Wikipedia...")
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        
        tables = soup.find_all('table', {'class': table_class})
        if not tables:
            logging.error(f"No tables with class '{table_class}' found for {name}.")
            return []

        correct_df = None
        for table in tables:
            try:
                df = pd.read_html(io.StringIO(str(table)))[0]
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(-1) # Take the last level of headers
                
                if ticker_col in df.columns:
                    correct_df = df
                    break # We found the right table, stop searching
            except Exception:
                continue # Ignore tables pandas can't parse

        if correct_df is None:
            logging.error(f"Found tables for {name}, but none contained the ticker column '{ticker_col}'.")
            return []
            
        tickers = correct_df[ticker_col].astype(str).apply(config['clean_fn']).tolist()
        logging.info(f"Found {len(tickers)} tickers for {name}.")
        return tickers

    except Exception as e:
        logging.error(f"An error occurred while scraping {name}: {e}")
        return []

def get_russell_2000_from_ishares():
    """
    DEFINITIVE FIX: Scrapes Russell 2000 components directly from the iShares (IWM)
    ETF holdings CSV file. This is a primary source and is extremely reliable.
    """
    logging.info("Scraping Russell 2000 tickers from primary source (iShares)...")
    url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    try:
        # iShares requires a standard browser user-agent
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        
        # Read the CSV data directly into pandas, skipping the header rows
        # The actual data starts after a line containing "Ticker"
        content = response.content.decode('utf-8')
        first_data_line = content.find("Ticker")
        
        if first_data_line == -1:
            raise ValueError("Could not find the header 'Ticker' in the downloaded CSV.")

        df = pd.read_csv(io.StringIO(content[first_data_line:]))
        
        # Drop any rows where the Ticker is NaN (e.g., summary rows at the bottom)
        df.dropna(subset=['Ticker'], inplace=True)
        
        tickers = df['Ticker'].tolist()
        logging.info(f"Found {len(tickers)} Russell 2000 tickers from iShares.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to scrape Russell 2000 tickers from iShares. Error: {e}")
        return []

# The data fetching function remains the same, it is robust.
# Included here for a complete, runnable script.
def fetch_data_for_index(index_name, tickers):
    if not tickers:
        logging.warning(f"No tickers provided for {index_name}. Skipping fetch process.")
        return
    index_dir = os.path.join(OUTPUT_DIR, index_name.replace(' ', '_'))
    os.makedirs(index_dir, exist_ok=True)
    logging.info(f"--- Starting data fetch for {index_name} ---")
    all_info_data, all_financials_data, all_historical_data = [], [], []
    total_tickers = len(tickers)
    for i, ticker_symbol in enumerate(tickers):
        logging.info(f"Processing {index_name}: {ticker_symbol} ({i+1}/{total_tickers})")
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            if info.get('regularMarketPrice') is not None:
                all_info_data.append(info)
            else:
                logging.warning(f"Could not retrieve valid info for {ticker_symbol}. Skipping.")
                time.sleep(SLEEP_TIMER)
                continue
            hist = ticker.history(period=HISTORICAL_DATA_PERIOD)
            if not hist.empty:
                hist.reset_index(inplace=True); hist['Ticker'] = ticker_symbol
                all_historical_data.append(hist)
            for st_type in ['financials', 'balance_sheet', 'cashflow']:
                df = getattr(ticker, st_type)
                if not df.empty:
                    df = df.T.reset_index(); df['Ticker'] = ticker_symbol
                    df['Statement'] = st_type.replace('_', ' ').title()
                    all_financials_data.append(df)
        except Exception as e:
            logging.error(f"Failed to process {ticker_symbol}. Error: {e}")
        time.sleep(SLEEP_TIMER)
    logging.info(f"--- Saving data for {index_name} ---")
    if all_info_data:
        pd.DataFrame(all_info_data).set_index('symbol').to_csv(os.path.join(index_dir, f"{index_name.replace(' ', '_')}_info.csv"))
    if all_historical_data:
        pd.concat(all_historical_data, ignore_index=True).to_csv(os.path.join(index_dir, f"{index_name.replace(' ', '_')}_historical_data.csv"), index=False)
    if all_financials_data:
        pd.concat(all_financials_data, ignore_index=True).rename(columns={'index': 'Date'}).to_csv(os.path.join(index_dir, f"{index_name.replace(' ', '_')}_financials.csv"), index=False)
    logging.info(f"--- Completed data fetch for {index_name} ---")


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for config in INDEX_CONFIG.values():
        tickers = get_tickers_from_wikipedia_robust(config)
        fetch_data_for_index(config['name'], tickers)
        
    russell_tickers = get_russell_2000_from_ishares()
    fetch_data_for_index("Russell 2000", russell_tickers)
    logging.info("All fetching processes are complete.")