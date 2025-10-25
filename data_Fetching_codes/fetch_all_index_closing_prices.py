import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
import re

# --- Configuration ---
# Configure logging to see progress and handle errors gracefully
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# The source of truth for all indices
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_stock_market_indices"

# Desired time period and output file
PERIOD = "3y"
INTERVAL = "1d"
OUTPUT_CSV = "all_world_indices_3y_daily.csv"


def scrape_all_index_tickers():
    """
    Scrapes the Wikipedia page for a comprehensive list of all stock market indices
    and their corresponding ticker symbols.
    
    Returns:
        A pandas DataFrame with 'Index' names and their cleaned 'Ticker' symbols.
    """
    logging.info(f"Scraping index list from: {WIKI_URL}")
    try:
        response = requests.get(WIKI_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch Wikipedia page: {e}")
        return None

    soup = BeautifulSoup(response.text, 'lxml')
    
    # Find all tables with the 'wikitable' class, which contain the indices
    tables = soup.find_all('table', {'class': 'wikitable'})
    
    if not tables:
        logging.error("No wikitables found on the page. The page structure might have changed.")
        return None
        
    all_indices_df = pd.DataFrame()
    
    # Use pandas to read all tables and concatenate them
    for table in tables:
        try:
            df = pd.read_html(str(table))[0]
            all_indices_df = pd.concat([all_indices_df, df], ignore_index=True)
        except Exception as e:
            logging.warning(f"Could not parse a table. Skipping it. Error: {e}")

    # --- Data Cleaning and Ticker Standardization ---

    # We need the 'Index' and 'Ticker symbol' columns. Let's find them.
    # Column names can vary slightly, so we search for them flexibly.
    ticker_col = next((col for col in all_indices_df.columns if 'Ticker' in col), None)
    index_col = 'Index'

    if not ticker_col or index_col not in all_indices_df.columns:
        logging.error("Could not find 'Index' or 'Ticker symbol' columns in the scraped data.")
        return None

    # Keep only the essential columns and drop rows with no ticker
    all_indices_df = all_indices_df[[index_col, ticker_col]].copy()
    all_indices_df.rename(columns={ticker_col: 'Ticker', index_col: 'Index'}, inplace=True)
    all_indices_df.dropna(subset=['Ticker'], inplace=True)
    all_indices_df = all_indices_df[all_indices_df['Ticker'] != '–'] # Remove empty placeholders

    # Clean the ticker symbols
    def clean_ticker(ticker_str):
        # Remove citations like [1], [a], etc.
        cleaned = re.sub(r'\[.*?\]', '', ticker_str)
        # Take the first ticker if multiple are listed
        cleaned = cleaned.split(',')[0].strip()
        # Yahoo Finance often uses a caret '^' for indices. Add it if not present.
        if not cleaned.startswith('^'):
            cleaned = '^' + cleaned
        return cleaned

    all_indices_df['Ticker'] = all_indices_df['Ticker'].apply(clean_ticker)
    
    # Remove any duplicates that may have arisen
    all_indices_df.drop_duplicates(subset=['Ticker'], inplace=True)
    
    logging.info(f"Successfully scraped and cleaned {len(all_indices_df)} unique index tickers.")
    return all_indices_df


def fetch_index_data(indices_df):
    """
    Fetches historical closing prices for the provided list of index tickers.
    
    Args:
        indices_df: A DataFrame containing 'Index' names and 'Ticker' symbols.
    """
    if indices_df is None or indices_df.empty:
        logging.error("No index data to fetch. Aborting.")
        return

    tickers_list = indices_df['Ticker'].tolist()
    logging.info(f"Attempting to download {len(tickers_list)} indices for the period '{PERIOD}'...")

    # yfinance's download function is highly optimized for batch requests
    data = yf.download(tickers_list, period=PERIOD, interval=INTERVAL, auto_adjust=True)

    if data.empty:
        logging.error("No data was returned from Yahoo Finance. Check tickers or network connection.")
        return

    # We only care about the closing price
    closing_prices = data['Close']
    
    # Drop columns that are completely empty (for tickers yfinance couldn't find)
    closing_prices.dropna(axis=1, how='all', inplace=True)
    
    # Create a mapping from Ticker -> Index Name for readable column headers
    ticker_to_name_map = pd.Series(indices_df.Index.values, index=indices_df.Ticker).to_dict()
    
    # Rename columns from '^DJI' to 'Dow Jones Industrial Average'
    closing_prices.rename(columns=ticker_to_name_map, inplace=True)
    
    logging.info(f"Successfully downloaded data for {len(closing_prices.columns)} indices.")

    # Save the final, clean DataFrame to a CSV file
    try:
        closing_prices.to_csv(OUTPUT_CSV)
        logging.info(f"Data successfully saved to '{OUTPUT_CSV}'")
        print("\n--- Data Fetch Complete! ---")
        print(f"Saved to: {OUTPUT_CSV}")
        print("\n--- Sample of the final data (last 5 days): ---")
        print(closing_prices.tail())
    except Exception as e:
        logging.error(f"Failed to save data to CSV: {e}")


def main():
    """Main function to run the scraper and data fetcher."""
    indices_df = scrape_all_index_tickers()
    fetch_index_data(indices_df)


if __name__ == "__main__":
    main()