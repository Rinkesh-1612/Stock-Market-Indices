import pandas as pd
import yfinance as yf
from tqdm import tqdm
import time
import pytz
import pycountry_convert as pc
import requests
import os
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==============================================================================
# === THE PERMANENT FIX IS HERE ================================================
# ==============================================================================
# This logic makes the script self-aware of its location.

# Get the absolute path of the directory where this script is located (e.g., .../scripts/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the 'data' folder by going up one level and then into 'data'
OUTPUT_DIR = os.path.join(SCRIPT_DIR, '..', 'data')

# The input file will also be in the data folder
INPUT_CSV = os.path.join(OUTPUT_DIR, r"D:\stock-market-indices-v2\data\indices_with_full_names.csv") 
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "master_indices_list.csv")
# ==============================================================================
# === END OF FIX ===============================================================
# ==============================================================================


# --- Map Exchange Codes to Cities for Pinpoint Geolocation Accuracy ---
EXCHANGE_TO_CITY_MAP = {
    'NYQ': 'New York, USA', 'NMS': 'New York, USA', 'PCX': 'New York, USA', 'OPR': 'New York, USA',
    'CME': 'Chicago, USA', 'CBO': 'Chicago, USA',
    'TOR': 'Toronto, Canada', 'MEX': 'Mexico City, Mexico', 'SAO': 'São Paulo, Brazil',
    'BUE': 'Buenos Aires, Argentina', 'SGO': 'Santiago, Chile',
    'LSE': 'London, UK', 'FRA': 'Frankfurt, Germany', 'PAR': 'Paris, France', 'EBS': 'Zurich, Switzerland',
    'AMS': 'Amsterdam, Netherlands', 'MIL': 'Milan, Italy', 'MCE': 'Madrid, Spain',
    'STO': 'Stockholm, Sweden', 'ZRH': 'Zurich, Switzerland', 'HEL': 'Helsinki, Finland',
    'GER': 'Frankfurt, Germany', 'VIE': 'Vienna, Austria', 'BRU': 'Brussels, Belgium',
    'CPH': 'Copenhagen, Denmark', 'LIS': 'Lisbon, Portugal',
    'HKG': 'Hong Kong', 'NSI': 'Mumbai, India', 'BSE': 'Mumbai, India',
    'SHH': 'Shanghai, China', 'SHZ': 'Shenzhen, China',
    'JPX': 'Tokyo, Japan', 'KSC': 'Seoul, South Korea', 'TAI': 'Taipei, Taiwan',
    'SES': 'Singapore', 'ASX': 'Sydney, Australia', 'JNB': 'Johannesburg, South Africa',
    'IST': 'Istanbul, Turkey', 'MOW': 'Moscow, Russia', 'TA': 'Tel Aviv, Israel'
}

# --- Helper Functions (code is unchanged) ---
TIMEZONE_TO_COUNTRY_CODE = {tz: code for code, tzs in pytz.country_timezones.items() for tz in tzs}
GEO_CACHE = {}

def get_location_from_info(info):
    timezone_str = info.get("exchangeTimezoneName")
    if timezone_str in TIMEZONE_TO_COUNTRY_CODE:
        try:
            country_code = TIMEZONE_TO_COUNTRY_CODE[timezone_str]
            country_name = pc.country_alpha2_to_country_name(country_code)
            continent_code = pc.country_alpha2_to_continent_code(country_code)
            continent_name = pc.convert_continent_code_to_continent_name(continent_code)
            return {"Country": country_name, "Continent": continent_name}
        except Exception: pass
    return {"Country": "Global", "Continent": "Global"}

def get_lat_lon(location_name):
    if location_name in GEO_CACHE: return GEO_CACHE[location_name]
    if pd.isna(location_name): return None, None
    try:
        url = f'https://nominatim.openstreetmap.org/search?q={location_name}&format=json&limit=1'
        resp = requests.get(url, headers={'User-Agent': 'GlobalIndexMapper/1.0'})
        resp.raise_for_status()
        results = resp.json()
        if results:
            lat, lon = float(results[0]['lat']), float(results[0]['lon'])
            GEO_CACHE[location_name] = (lat, lon); return lat, lon
    except Exception as e:
        logging.error(f"Geolocation failed for {location_name}: {e}")
    GEO_CACHE[location_name] = (None, None); return None, None


def create_master_list_from_csv(input_file):
    try:
        df_input = pd.read_csv(input_file)
        df_input.rename(columns={'Full Index Name': 'Index Name', 'Yahoo Finance Ticker': 'Ticker'}, inplace=True)
    except FileNotFoundError:
        logging.error(f"FATAL: Input file not found at '{input_file}'. Please ensure 'verified_indices_input.csv' is in the 'data' folder.")
        return
        
    master_data = []
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    logging.info(f"Starting to process {len(df_input)} indices from '{input_file}'...")
    
    for _, row in tqdm(df_input.iterrows(), total=len(df_input), desc="Enriching Index Data"):
        index_name, ticker = row['Index Name'], row['Ticker']
        try:
            info = yf.Ticker(ticker).info
            location = get_location_from_info(info)
            country, continent = location['Country'], location['Continent']
            exchange_code = info.get('exchange')
            location_to_geocode = EXCHANGE_TO_CITY_MAP.get(exchange_code, country)
            lat, lon = get_lat_lon(location_to_geocode)
            master_data.append({"Index Name": index_name, "Ticker": ticker, "Country": country, "Continent": continent, "Latitude": lat, "Longitude": lon})
            time.sleep(0.1)
        except Exception as e:
            logging.warning(f"Could not process '{index_name}' ({ticker}). Reason: {e}")
            master_data.append({"Index Name": index_name, "Ticker": ticker, "Country": "Unknown", "Continent": "Unknown", "Latitude": None, "Longitude": None})

    df_master = pd.DataFrame(master_data)
    df_master.to_csv(OUTPUT_FILE, index=False)
    
    logging.info("\n" + "="*50)
    logging.info(f"Master Index List Creation Complete! File saved correctly to:")
    logging.info(os.path.abspath(OUTPUT_FILE)) # Print the full, absolute path for confirmation
    logging.info("="*50)

    print("\n--- Sample of the final master data ---")
    print(df_master.head())

if __name__ == "__main__":
    create_master_list_from_csv(INPUT_CSV)