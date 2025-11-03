import pandas as pd
import time
import requests
import io
import os
import re
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data')
INPUT_CSV = os.path.join(DATA_DIR, "master_indices_list.csv")
OUTPUT_DIR_INDIVIDUAL = os.path.join(DATA_DIR, "constituent_lists_raw") # Saving to a new 'raw' folder
SUMMARY_REPORT_FILE = os.path.join(DATA_DIR, "constituent_scraping_summary_raw.csv")

# Dictionaries for Manual Overrides, Skipping, and Proxies (Unchanged)
MANUAL_URL_OVERRIDES = {
    "^DJGT": "https://en.wikipedia.org/wiki/Dow_Jones_Global_Titans_50",
    "^GDOW": "https://en.wikipedia.org/wiki/The_Global_Dow",
    "^MERV": "https://en.wikipedia.org/wiki/MERVAL",
    "^IPSA": "https://en.wikipedia.org/wiki/%C3%8Dndice_de_Precios_Selectivo_de_Acciones",
    "^MXX": "https://en.wikipedia.org/wiki/Indice_de_Precios_y_Cotizaciones",
}
SKIP_INDICES = ["^SPG1200", "^VIX"]
PROXY_MAP = {"^IXIC": "^NDX"}

# --- NEW: Exchange Suffix Map ---
# This dictionary is crucial for cleaning tickers from non-US indices
EXCHANGE_SUFFIX_MAP = {
    # Europe
    '^OMXC25': '.CO', '^OMXH25': '.HE', '^FCHI': '.PA', '^CN20': '.PA', '^SBF120': '.PA',
    '^GDAXI': '.DE', '^MDAXI': '.DE', '^TECDAX': '.DE', '^ISEQ': '.IR', 'FTSEMIB.MI': '.MI',
    '^AEX': '.AS', '^AMX': '.AS', 'PSI20.LS': '.LS', '^IBEX': '.BC', '^OMX': '.ST',
    '^SSMI': '.SW', '^FTSE': '.L', '^FTMC': '.L', '^FTAS': '.L', '^BFX': '.BR', '^ATX': '.VI',
    'XU100.IS': '.IS',
    # Asia-Pacific
    '^HSI': '.HK', '^NSEI': '.NS', '^NSMIDCP': '.NS', '^BSESN': '.BO', '^JKSE': '.JK',
    '^N225': '.T', '^KLSE': '.KL', '^TASI.SR': '.SR', '^STI': '.SI', '^KS11': '.KS',
    '^TWII': '.TW', '^SET.BK': '.BK', '^AORD': '.AX', '^AXJO': '.AX', '^AXKO': '.AX',
    '^NZ50': '.NZ'
}

TICKER_COLUMN_CANDIDATES = ['Ticker', 'Symbol', 'Ticker symbol']
COMPANY_COLUMN_CANDIDATES = ['Company', 'Name', 'Security', 'Corporation']
LANDMARK_HEADINGS = ['Components', 'Constituents', 'List of companies', 'Composition']

# (find_best_wikipedia_url and other helper functions remain largely the same, but simplified)

def find_constituents_table_or_list(soup):
    """Finds the most likely constituent data, whether it's in a table or a list."""
    # (This logic is the same as before, combining table and list finding)
    for heading_text in LANDMARK_HEADINGS:
        span = soup.find('span', class_='mw-headline', string=re.compile(f'^{re.escape(heading_text)}', re.IGNORECASE))
        if span:
            heading_tag = span.find_parent(['h2', 'h3'])
            if heading_tag:
                table = heading_tag.find_next('table', {'class': 'wikitable'})
                if table:
                    df, company_col, ticker_col = parse_table_for_columns(table)
                    if df is not None:
                        return ('table', (df, company_col, ticker_col))
    # Fallback to general table search
    all_tables = soup.find_all('table', {'class': 'wikitable'})
    for table in all_tables:
        df, company_col, ticker_col = parse_table_for_columns(table)
        if df is not None: return ('table', (df, company_col, ticker_col))
            
    # Fallback to list/navbox search
    list_div = soup.find('div', {'class': 'div-col'}) or soup.find('div', {'class': 'navbox'})
    if list_div:
        items = list_div.find_all('a')
        if items:
            names = [item.get_text(strip=True) for item in items if len(item.get_text(strip=True)) > 2]
            if names: return ('list', names)
            
    return None, None

def parse_table_for_columns(table_tag):
    # (Same as before)
    try:
        df = pd.read_html(io.StringIO(str(table_tag)))[0]
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
        ticker_col = next((col for col in df.columns if col in TICKER_COLUMN_CANDIDATES), None)
        company_col = next((col for col in df.columns if col in COMPANY_COLUMN_CANDIDATES), None)
        if ticker_col and company_col: return df, company_col, ticker_col
    except: pass
    return None, None, None

def clean_ticker(raw_ticker, suffix):
    """A more robust function to clean and apply the correct exchange suffix."""
    # Handles cases like "NYSE: MMM", "CARL B", "GOOG;GOOGL"
    # We take the first part before any space or semicolon
    ticker = str(raw_ticker).split(';')[0].split(' ')[-1].strip()
    return ticker + suffix

def main():
    os.makedirs(OUTPUT_DIR_INDIVIDUAL, exist_ok=True)
    master_list = pd.read_csv(INPUT_CSV)
    summary_report = []

    logging.info("--- Starting Phase 2a: RAW Constituent Capture ---")

    for _, row in master_list.iterrows():
        index_name, index_ticker = row['Index Name'], row['Ticker']
        
        print("\n" + "="*70)
        logging.info(f"Processing Index: {index_name} ({index_ticker})")
        
        safe_ticker_name = re.sub(r'[\^.:]', '_', index_ticker)
        filepath = os.path.join(OUTPUT_DIR_INDIVIDUAL, f"{safe_ticker_name}.csv")

        if os.path.exists(filepath):
            logging.info(f"  ✅ Raw constituent file already exists. Skipping.")
            summary_report.append({"Index Name": index_name, "Status": "Skipped (Exists)", "Scraped Count": "N/A", "Source URL": "N/A"})
            continue

        status, scraped_count, url = "Failed", 0, "N/A"
        
        if index_ticker in SKIP_INDICES:
            status = "Skipped (No Source)"
        # (Proxy logic will be handled later, we want to scrape the real list first if available)
        else:
            url = MANUAL_URL_OVERRIDES.get(index_ticker) or find_best_wikipedia_url(index_name)
            if url:
                logging.info(f"  - Using URL: {url}")
                try:
                    response = requests.get(url, headers={'User-Agent': 'MyCoolTool/1.0'})
                    soup = BeautifulSoup(response.text, 'lxml')
                    data_type, raw_data = find_constituents_table_or_list(soup)

                    if data_type == 'table':
                        df, company_col, ticker_col = raw_data
                        suffix = EXCHANGE_SUFFIX_MAP.get(index_ticker, '') # Get the correct suffix
                        
                        constituents_df = pd.DataFrame()
                        constituents_df['Company Name'] = df[company_col]
                        constituents_df['Raw Ticker'] = df[ticker_col]
                        constituents_df['Cleaned Ticker'] = df[ticker_col].apply(lambda t: clean_ticker(t, suffix))
                        
                        status = "Success (Table)"
                        constituents_df.to_csv(filepath, index=False)
                        scraped_count = len(constituents_df)
                        logging.info(f"  ✅ CAPTURED {scraped_count} constituents to '{filepath}'")

                    elif data_type == 'list':
                        # For lists, we just have names. Ticker enrichment comes later.
                        constituents_df = pd.DataFrame(raw_data, columns=['Company Name'])
                        status = "Success (List)"
                        constituents_df.to_csv(filepath, index=False)
                        scraped_count = len(constituents_df)
                        logging.info(f"  ✅ CAPTURED {scraped_count} company names to '{filepath}'")
                        
                    else:
                        logging.error("  - Could not find any valid table or list.")
                except Exception as e:
                    logging.error(f"  - An unexpected error occurred: {e}")
            else:
                status = "No URL Found"

        summary_report.append({"Index Name": index_name, "Status": status, "Scraped Count": scraped_count, "Source URL": url or "N/A"})

    # (Final report generation remains the same)
    logging.info("\n\n" + "="*80)
    report_df = pd.DataFrame(summary_report)
    print(report_df.to_string())
    report_df.to_csv(SUMMARY_REPORT_FILE, index=False)

# (Find best url function to be placed here)
def find_best_wikipedia_url(index_name):
    search_query = re.sub(r'\(.*\)', '', index_name).strip()
    search_url = "https://en.wikipedia.org/w/api.php"
    params = {"action": "query", "list": "search", "srsearch": search_query, "srlimit": 1, "format": "json"}
    try:
        response = requests.get(search_url, params=params, headers={'User-Agent': 'MyCoolTool/1.0'})
        response.raise_for_status()
        data = response.json()
        if data['query']['search']:
            page_title = data['query']['search'][0]['title']
            if query_is_relevant(search_query, page_title):
                return f"https://en.wikipedia.org/wiki/{page_title.replace(' ', '_')}"
    except: pass
    return None

def query_is_relevant(query, title):
    query_parts = set(query.lower().split())
    title_parts = set(title.lower().split())
    return len(query_parts.intersection(title_parts)) > 0
    
if __name__ == "__main__":
    main()