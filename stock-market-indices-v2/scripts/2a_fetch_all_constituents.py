import pandas as pd
import yfinance as yf
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
OUTPUT_DIR_INDIVIDUAL = os.path.join(DATA_DIR, "constituent_lists")
OUTPUT_FILE_MASTER = os.path.join(DATA_DIR, "master_constituents_list.csv")
SUMMARY_REPORT_FILE = os.path.join(DATA_DIR, "constituent_scraping_summary.csv")

# =====================================================================================
# === THE GROUND-TRUTH CONFIGURATION HUB (Based on your research) ====================
# =====================================================================================
# =====================================================================================
# === THE GROUND-TRUTH CONFIGURATION HUB (v2 - EXPANDED) ==============================
# =====================================================================================
INDEX_CONFIG = {
    # --- Americas ---
    '^DJI': {'url': 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Symbol'},
    '^DJT': {'url': 'https://en.wikipedia.org/wiki/Dow_Jones_Transportation_Average', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^DJU': {'url': 'https://en.wikipedia.org/wiki/Dow_Jones_Utility_Average', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^NDX': {'url': 'https://en.wikipedia.org/wiki/Nasdaq-100', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^RUI': {'url': 'https://en.wikipedia.org/wiki/Russell_1000_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Symbol'},
    '^GSPC': {'url': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'type': 'table', 'company_col': 'Security', 'ticker_col': 'Symbol', 'clean_fn': lambda s: s.replace('.', '-')},
    '^OEX': {'url': 'https://en.wikipedia.org/wiki/S%26P_100', 'type': 'table', 'company_col': 'Name', 'ticker_col': 'Symbol'},
    '^MID': {'url': 'https://en.wikipedia.org/wiki/S%26P_400', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker Symbol'},
    '^BVSP': {'url': 'https://en.wikipedia.org/wiki/List_of_companies_listed_on_B3', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker', 'suffix': '.SA'},
    '^GSPTSE': {'url': 'https://en.wikipedia.org/wiki/S%26P/TSX_Composite_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^IPSA': {'url': 'https://en.wikipedia.org/wiki/%C3%8Dndice_de_Precios_Selectivo_de_Acciones', 'type': 'table', 'company_col': 'Empresa', 'ticker_col': 'Símbolo', 'suffix': '.SN'},
    '^MXX': {'url': 'https://en.wikipedia.org/wiki/Indice_de_Precios_y_Cotizaciones', 'type': 'table', 'company_col': 'Name', 'ticker_col': 'Symbol'},
    '^RUT': {'type': 'ishares_csv', 'name': 'Russell 2000'},
    '^MERV': {'url': 'https://en.wikipedia.org/wiki/MERVAL', 'type': 'navbox_list', 'company_col': 'Company Name', 'suffix': '.BA'},
    
    # --- Asia-Pacific ---
    '000300.SS': {'url': 'https://en.wikipedia.org/wiki/CSI_300_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '000016.SS': {'url': 'https://en.wikipedia.org/wiki/SSE_50_Index', 'type': 'table', 'company_col': 'Name', 'ticker_col': 'Ticker symbol'},
    '000001.SS': {'url': 'https://en.wikipedia.org/wiki/SSE_Composite_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker Symbol'},
    '^HSI': {'url': 'https://en.wikipedia.org/wiki/Hang_Seng_Index', 'type': 'table', 'company_col': 'Name', 'ticker_col': 'Ticker', 'clean_fn': lambda s: f"{int(s.split(':')[-1].strip()):04d}.HK"},
    '^BSESN': {'url': 'https://en.wikipedia.org/wiki/BSE_SENSEX', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Symbol', 'suffix': '.BO'},
    '^NSEI': {'url': 'https://en.wikipedia.org/wiki/NIFTY_50', 'type': 'table', 'company_col': 'Company name', 'ticker_col': 'Symbol', 'suffix': '.NS'},
    '^NSMIDCP': {'url': 'https://en.wikipedia.org/wiki/NIFTY_Next_50', 'type': 'table', 'company_col': 'Company name', 'ticker_col': 'Symbol', 'suffix': '.NS'},
    '^N225': {'url': 'https://en.wikipedia.org/wiki/Nikkei_225', 'type': 'table', 'company_col': 'Company Name', 'ticker_col': 'Symbol', 'suffix': '.T'},
    '^KLSE': {'url': 'https://en.wikipedia.org/wiki/FTSE_Bursa_Malaysia_KLCI', 'type': 'table', 'company_col': 'Constituent Name', 'ticker_col': 'Stock Code', 'suffix': '.KL'},
    '^STI': {'url': 'https://en.wikipedia.org/wiki/Straits_Times_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Stock symbol', 'suffix': '.SI'},
    '^SET.BK': {'url': 'https://en.wikipedia.org/wiki/SET50_Index_and_SET100_Index', 'type': 'table', 'company_col': 'Securities Name', 'ticker_col': 'Symbol', 'suffix': '.BK'},
    '^AXJO': {'url': 'https://en.wikipedia.org/wiki/S%26P/ASX_200', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Code', 'suffix': '.AX'},
    '^NZ50': {'url': 'https://en.wikipedia.org/wiki/S%26P/NZX_50', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker symbol', 'suffix': '.NZ'},
    '^KS11': {'url': 'https://en.wikipedia.org/wiki/KOSPI', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^TWII': {'url': 'https://en.wikipedia.org/wiki/Taiwan_Capitalization_Weighted_Stock_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Symbol', 'suffix': '.TW'},
    '^AORD': {'url': 'https://en.wikipedia.org/wiki/All_Ordinaries', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Code', 'suffix': '.AX'},
    
    # --- Europe ---
    '^STOXX50E': {'url': 'https://en.wikipedia.org/wiki/EURO_STOXX_50', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^STOXX': {'url': 'https://en.wikipedia.org/wiki/STOXX_Europe_600', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^ATX': {'url': 'https://en.wikipedia.org/wiki/Austrian_Traded_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker', 'suffix': '.VI'},
    '^BFX': {'url': 'https://en.wikipedia.org/wiki/BEL_20', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker', 'suffix': '.BR'},
    '^OMXC25': {'url': 'https://en.wikipedia.org/wiki/OMX_Copenhagen_25', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker symbol', 'suffix': '.CO'},
    '^OMXH25': {'url': 'https://en.wikipedia.org/wiki/OMX_Helsinki_25', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker', 'suffix': '.HE'},
    '^FCHI': {'url': 'https://en.wikipedia.org/wiki/CAC_40', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker', 'suffix': '.PA'},
    '^CN20': {'url': 'https://en.wikipedia.org/wiki/CAC_Next_20', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker symbol', 'suffix': '.PA'},
    '^GDAXI': {'url': 'https://en.wikipedia.org/wiki/DAX', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker', 'suffix': '.DE'},
    '^MDAXI': {'url': 'https://en.wikipedia.org/wiki/MDAX', 'type': 'table', 'company_col': 'Name', 'ticker_col': 'Symbol', 'suffix': '.DE'},
    '^TECDAX': {'url': 'https://en.wikipedia.org/wiki/TecDAX', 'type': 'table', 'company_col': 'Name', 'ticker_col': 'Symbol', 'suffix': '.DE'},
    'FTSEMIB.MI': {'url': 'https://en.wikipedia.org/wiki/FTSE_MIB', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^AEX': {'url': 'https://en.wikipedia.org/wiki/AEX_index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker symbol', 'suffix': '.AS'},
    '^AMX': {'url': 'https://en.wikipedia.org/wiki/AMX_index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker symbol', 'suffix': '.AS'},
    'PSI20.LS': {'url': 'https://en.wikipedia.org/wiki/PSI-20', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker', 'suffix': '.LS'},
    '^IBEX': {'url': 'https://en.wikipedia.org/wiki/IBEX_35', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^OMX': {'url': 'https://en.wikipedia.org/wiki/OMX_Stockholm_30', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Symbol', 'suffix': '.ST'},
    '^SSMI': {'url': 'https://en.wikipedia.org/wiki/Swiss_Market_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    '^FTSE': {'url': 'https://en.wikipedia.org/wiki/FTSE_100_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker', 'suffix': '.L'},
    '^FTMC': {'url': 'https://en.wikipedia.org/wiki/FTSE_250_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker'},
    'XU100.IS': {'url': 'https://en.wikipedia.org/wiki/BIST_100_Index', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Ticker', 'suffix': '.IS'},
    '^CASE30': {'url': 'https://en.wikipedia.org/wiki/EGX_30', 'type': 'table', 'company_col': 'Company', 'ticker_col': 'Reuters Code'},

    # --- Global / Raw List Types ---
    '^DJGT': {'url': 'https://en.wikipedia.org/wiki/Dow_Jones_Global_Titans_50', 'type': 'table', 'company_col': 'Corporation', 'ticker_col': 'Ticker'},
    '^SPG100': {'url': 'https://en.wikipedia.org/wiki/S%26P_Global_100', 'type': 'raw_list', 'company_col': 'Company Name'},
    '^GDOW': {'url': 'https://en.wikipedia.org/wiki/The_Global_Dow', 'type': 'raw_list', 'company_col': 'Company Name'},

    # --- Other/Special ---
    '^HUI': {'url': 'https://en.wikipedia.org/wiki/HUI_Gold_Index', 'type': 'table', 'company_col': 'Company name', 'ticker_col': 'Symbol'},
    '^XAU': {'url': 'https://en.wikipedia.org/wiki/Philadelphia_Gold_and_Silver_Index', 'type': 'table', 'company_col': 'Name', 'ticker_col': 'Trading Symbol'},
    
    # --- Skipped / Proxied ---
    '^SPG1200': {'type': 'skip', 'reason': 'No reliable list page'},
    '^VIX': {'type': 'skip', 'reason': 'Volatility measure, no companies'},
    '^IXIC': {'type': 'proxy', 'use': '^NDX', 'reason': 'No list, but 80% overlap with NASDAQ-100'}
}

# --- Main Functions ---

def scrape_table(config):
    response = requests.get(config['url'], headers={'User-Agent': 'MyCoolTool/1.0'})
    soup = BeautifulSoup(response.text, 'lxml')
    tables = soup.find_all('table', {'class': 'wikitable'})
    
    for table in tables:
        try:
            df = pd.read_html(io.StringIO(str(table)))[0]
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(-1)
            
            if config['company_col'] in df.columns and config['ticker_col'] in df.columns:
                const_df = df[[config['company_col'], config['ticker_col']]]
                const_df.columns = ['Company Name', 'Raw Ticker']
                
                # Apply cleaning functions
                suffix = config.get('suffix', '')
                if 'clean_fn' in config:
                    const_df['Company Ticker'] = const_df['Raw Ticker'].apply(config['clean_fn'])
                else:
                    const_df['Company Ticker'] = const_df['Raw Ticker'].astype(str) + suffix
                
                return const_df[['Company Name', 'Company Ticker']]
        except:
            continue
    return pd.DataFrame()


def scrape_raw_list(config):
    response = requests.get(config['url'], headers={'User-Agent': 'MyCoolTool/1.0'})
    soup = BeautifulSoup(response.text, 'lxml')
    content_div = soup.find('div', {'id': 'mw-content-text'})
    # This is a heuristic: find all list items in the main content.
    items = content_div.find_all('li')
    names = [item.get_text(strip=True).split('(')[0].strip() for item in items if len(item.get_text(strip=True)) > 2]
    # For raw lists, we'll try to find tickers in a later step
    return pd.DataFrame(names, columns=['Company Name'])


def get_ishares_csv(name):
    url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=20)
        content = response.content.decode('utf-8')
        first_data_line = content.find("Ticker")
        df = pd.read_csv(io.StringIO(content[first_data_line:]))
        df.dropna(subset=['Ticker'], inplace=True)
        df.rename(columns={'Name': 'Company Name', 'Ticker': 'Company Ticker'}, inplace=True)
        return df[['Company Name', 'Company Ticker']]
    except Exception as e:
        logging.error(f"Failed to scrape {name} from iShares: {e}")
        return pd.DataFrame()


def main():
    os.makedirs(OUTPUT_DIR_INDIVIDUAL, exist_ok=True)
    master_list = pd.read_csv(INPUT_CSV)
    summary_report = []
    all_constituents_dfs = []

    logging.info("--- Starting Phase 2a: CONFIG-DRIVEN Constituent Scraping ---")

    for _, row in master_list.iterrows():
        index_name, index_ticker = row['Index Name'], row['Ticker']
        
        print("\n" + "="*70)
        logging.info(f"Processing Index: {index_name} ({index_ticker})")

        safe_ticker_name = re.sub(r'[\^.:]', '_', index_ticker)
        filepath = os.path.join(OUTPUT_DIR_INDIVIDUAL, f"{safe_ticker_name}.csv")

        if os.path.exists(filepath):
            logging.info(f"  ✅ Constituent file already exists. Skipping.")
            continue

        config = INDEX_CONFIG.get(index_ticker)
        status, scraped_count = "Failed", 0
        df_constituents = pd.DataFrame()

        if not config:
            status = "No Config"
        elif config['type'] == 'skip':
            status = f"Skipped ({config['reason']})"
        elif config['type'] == 'proxy':
            status = f"Proxy ({config['use']})" # We will handle this in the next script
        elif config['type'] == 'ishares_csv':
            df_constituents = get_ishares_csv(config['name'])
            status = "Success (iShares)"
        elif config['type'] == 'table':
            df_constituents = scrape_table(config)
            status = "Success (Table)"
        elif config['type'] == 'raw_list':
            df_constituents = scrape_raw_list(config)
            status = "Success (Raw List)"

        scraped_count = len(df_constituents)
        if scraped_count > 0:
            df_constituents.to_csv(filepath, index=False)
            logging.info(f"  ✅ Saved {scraped_count} constituents to '{filepath}'")
            
        summary_report.append({"Index Name": index_name, "Status": status, "Scraped Count": scraped_count, "Source URL": config.get('url', 'N/A') if config else 'N/A'})

    # (Final report generation remains the same)
    logging.info("\n\n" + "="*80)
    logging.info("--- CONFIG-DRIVEN SCRAPING SUMMARY ---")
    report_df = pd.DataFrame(summary_report)
    print(report_df.to_string())
    report_df.to_csv(SUMMARY_REPORT_FILE, index=False)
    logging.info(f"\n✅ Full summary report saved to '{os.path.abspath(SUMMARY_REPORT_FILE)}'")

if __name__ == "__main__":
    main()