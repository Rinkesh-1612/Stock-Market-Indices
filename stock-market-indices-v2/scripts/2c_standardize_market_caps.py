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
# This script will be run *after* the main data fetch, using its output
COMPANY_INFO_FILE = os.path.join(DATA_DIR, "final_company_info.csv")

# --- Outputs ---
# We create a new, final version of the company info file
FINAL_COMPANY_INFO_USD_CSV = os.path.join(DATA_DIR, "final_company_info_usd.csv")

def get_usd_conversion_rates(currencies_to_fetch):
    """Fetches the latest conversion rates for a list of currencies to USD."""
    rates = {'USD': 1.0} # USD to USD is always 1
    logging.info(f"Fetching conversion rates for {len(currencies_to_fetch)} currencies to USD...")
    
    for currency in tqdm(currencies_to_fetch, desc="Fetching Rates"):
        if currency == 'USD' or pd.isna(currency):
            continue
        try:
            # yfinance uses a standard format like 'EURUSD=X' for exchange rates
            rate_ticker = f"{currency}USD=X"
            rate_info = yf.Ticker(rate_ticker).info
            # Use a fallback chain to find the most recent price
            rate = rate_info.get('regularMarketPrice') or rate_info.get('previousClose')
            if rate:
                rates[currency] = rate
            else:
                rates[currency] = None
                logging.warning(f"  - Could not find a valid rate for {currency}.")
            time.sleep(0.1)
        except Exception as e:
            logging.warning(f"  - Error fetching rate for {currency}: {e}")
            rates[currency] = None
    return rates

def main():
    try:
        df_info = pd.read_csv(COMPANY_INFO_FILE)
    except FileNotFoundError:
        logging.error(f"FATAL: Input file not found at '{COMPANY_INFO_FILE}'. Please run script 3 first.")
        return

    # Find all unique currencies we need to convert
    unique_currencies = [c for c in df_info['Currency'].unique() if c != 'USD']
    
    conversion_rates = get_usd_conversion_rates(unique_currencies)
    
    logging.info("Applying conversion rates to calculate MarketCap in USD...")

    # Define a function to apply the conversion
    def convert_to_usd(row):
        rate = conversion_rates.get(row['Currency'])
        if rate is not None:
            return row['MarketCap'] * rate # Divide local currency value by the rate (e.g., INR per USD)
        elif row['Currency'] == 'USD':
            return row['MarketCap']
        return None # Return None if conversion is not possible

    df_info['MarketCap_USD'] = df_info.apply(convert_to_usd, axis=1)

    # Clean up and save
    df_info.dropna(subset=['MarketCap_USD'], inplace=True)
    df_info = df_info[df_info['MarketCap_USD'] > 0]
    
    df_info.to_csv(FINAL_COMPANY_INFO_USD_CSV, index=False)

    logging.info("\n" + "="*50)
    logging.info("Market Cap Standardization Complete!")
    logging.info(f"Successfully converted and saved info for {len(df_info)} companies.")
    logging.info(f"Final file with USD market caps saved to: '{os.path.abspath(FINAL_COMPANY_INFO_USD_CSV)}'")
    logging.info("="*50)


if __name__ == "__main__":
    main()