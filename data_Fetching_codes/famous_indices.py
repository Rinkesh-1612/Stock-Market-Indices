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
HISTORICAL_DATA_PERIOD = "1y"
SLEEP_TIMER = 1.2
OUTPUT_DIR = "index_market_data"

# --- Advanced, Configuration-Driven Ticker Scraping Engine (VERSION 3) ---

INDEX_CONFIG = {
    "sp500": {
        "name": "S&P 500",
        "url": "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        # CORRECTED: Using the specific table ID 'constituents' to avoid ambiguity
        "table_identifier": {'id': 'constituents'}, 
        "ticker_column": "Symbol",
        "clean_fn": lambda s: s.replace('.', '-')
    },

    "ftse100": {
        "name": "FTSE 100",
        "url": "https://en.wikipedia.org/wiki/FTSE_100_Index",
        "table_identifier": {'id': 'constituents'},
        "ticker_column": "EPIC",
        "clean_fn": lambda s: f"{s}.L" if '.' not in s else s
    },
    # --- NEW INDICES ADDED ---
    "nikkei225": {
        "name": "Nikkei 225",
        "url": "https://en.wikipedia.org/wiki/Nikkei_225",
        "table_identifier": {'class': 'wikitable sortable'},
        "ticker_column": "Symbol",
        # For Tokyo Stock Exchange, yfinance needs the ticker plus '.T'
        "clean_fn": lambda s: f"{s.split(': ')[1]}.T" if ': ' in s else f"{s}.T"
    },
    "hangseng": {
        "name": "Hang Seng",
        "url": "https://en.wikipedia.org/wiki/Hang_Seng_Index",
        "table_identifier": {'class': 'wikitable sortable'},
        "ticker_column": "Ticker",
        # For Hong Kong Stock Exchange, yfinance needs leading zeros and '.HK'
        "clean_fn": lambda s: f"{int(s):04d}.HK"
    }
    # Russell 2000 is still handled separately
}

def get_tickers_from_wikipedia(config):
    """Scrapes tickers from a Wikipedia page based on a flexible configuration."""
    name = config['name']
    url = config['url']
    logging.info(f"Scraping {name} tickers from Wikipedia...")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        
        table = soup.find('table', config['table_identifier'])
        if table is None:
            logging.error(f"Could not find the specified table for {name}. The page structure may have changed.")
            return []

        df = pd.read_html(io.StringIO(str(table)))[0]
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
        
        ticker_col = config['ticker_column']
        if ticker_col not in df.columns:
            logging.error(f"Ticker column '{ticker_col}' not found in the table for {name}. Available columns: {df.columns.tolist()}")
            return []
            
        tickers = df[ticker_col].astype(str).apply(config['clean_fn']).tolist()
        logging.info(f"Found {len(tickers)} tickers for {name}.")
        return tickers

    except Exception as e:
        logging.error(f"An error occurred while scraping {name}: {e}")
        return []

def get_russell_2000_tickers():
    """Scrapes Russell 2000 tickers from a non-Wikipedia source."""
    logging.info("Scraping Russell 2000 tickers... This may take a moment.")
    url = 'https://www.lazyfa.com/screener/russell-2000'
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find('table')
        tickers = []
        if table:
            for row in table.findAll('tr')[1:]:
                try:
                    ticker = row.findAll('td')[0].text.strip()
                    tickers.append(ticker)
                except IndexError:
                    continue
        logging.info(f"Found {len(tickers)} Russell 2000 tickers.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to scrape Russell 2000 tickers. The source may be down or has changed. Error: {e}")
        return []

def fetch_data_for_index(index_name, tickers):
    """Fetches info, historical data, and financials for a given list of tickers."""
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
                logging.warning(f"Could not retrieve valid info for {ticker_symbol}. It may be delisted or invalid. Skipping.")
                time.sleep(SLEEP_TIMER)
                continue

            hist = ticker.history(period=HISTORICAL_DATA_PERIOD)
            if not hist.empty:
                hist.reset_index(inplace=True)
                hist['Ticker'] = ticker_symbol
                all_historical_data.append(hist)
            
            for statement_type in ['financials', 'balance_sheet', 'cashflow']:
                df = getattr(ticker, statement_type)
                if not df.empty:
                    df = df.T.reset_index()
                    df['Ticker'] = ticker_symbol
                    df['Statement'] = statement_type.replace('_', ' ').title()
                    all_financials_data.append(df)

        except Exception as e:
            logging.error(f"Failed to process {ticker_symbol}. Error: {e}")
        
        time.sleep(SLEEP_TIMER)

    logging.info(f"--- Saving data for {index_name} ---")
    
    if all_info_data:
        info_df = pd.DataFrame(all_info_data).set_index('symbol')
        info_df.to_csv(os.path.join(index_dir, f"{index_name.replace(' ', '_')}_info.csv"))
        logging.info(f"Saved company info to {index_dir}")

    if all_historical_data:
        pd.concat(all_historical_data, ignore_index=True).to_csv(os.path.join(index_dir, f"{index_name.replace(' ', '_')}_historical_data.csv"), index=False)
        logging.info(f"Saved historical data to {index_dir}")

    if all_financials_data:
        pd.concat(all_financials_data, ignore_index=True).rename(columns={'index': 'Date'}).to_csv(os.path.join(index_dir, f"{index_name.replace(' ', '_')}_financials.csv"), index=False)
        logging.info(f"Saved financial statements to {index_dir}")

    logging.info(f"--- Completed data fetch for {index_name} ---")

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    for config in INDEX_CONFIG.values():
        tickers = get_tickers_from_wikipedia(config)
        fetch_data_for_index(config['name'], tickers)
        
    russell_tickers = get_russell_2000_tickers()
    fetch_data_for_index("Russell 2000", russell_tickers)

    logging.info("All fetching processes are complete.")