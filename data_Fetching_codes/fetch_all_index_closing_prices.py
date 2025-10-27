import yfinance as yf
import pandas as pd
import logging
from collections import OrderedDict

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
PERIOD = "3y"
INTERVAL = "1d"
OUTPUT_CSV = "global_indices_master_3y_daily.csv"
FAILED_LOG_FILE = "failed_to_fetch_indices.txt"

# --- The Comprehensive, Manually Curated Dictionary of World Indices ---
# Transcribed from your screenshot and mapped to yfinance tickers.
INDEX_TICKERS = OrderedDict([
    # == Global Indices ==
    ("Dow Jones Global Titans 50", "^DJGT"),
    ("FTSE All-World", "^FTAW"),
    ("S&P Global 100", "^SPG100"),
    ("S&P Global 1200", "^SPG1200"),
    ("The Global Dow", "^GDOW"),
    ("MSCI World", "URTH"),  # Using a major ETF as a reliable proxy for the index
    ("MSCI EAFE", "EFA"),    # ETF proxy for Europe, Australasia, Far East
    
    # == Regional Indices ==
    ("S&P Asia 50", "^SPAS50"),
    ("EURO STOXX 50", "^STOXX50E"),
    ("STOXX Europe 600", "^STOXX"),
    ("S&P Europe 350", "^SPE350"),
    ("S&P Latin America 40", "^SPLAC"),

    # == Americas ==
    ("S&P 500 (USA)", "^GSPC"),
    ("Dow Jones Industrial Average (USA)", "^DJI"),
    ("NASDAQ Composite (USA)", "^IXIC"),
    ("Russell 2000 (USA)", "^RUT"),
    ("CBOE Volatility Index (VIX)", "^VIX"),
    ("MERVAL (Argentina)", "^MERV"),
    ("Bovespa Index (Brazil)", "^BVSP"),
    ("S&P/TSX Composite (Canada)", "^GSPTSE"),
    ("IPSA (Chile)", "^IPSA"),
    ("COLCAP (Colombia)", "^COLCAP"),
    ("IPC (Mexico)", "^MXX"),
    ("S&P/BVL Peru General", "^SPBLPGPT"),
    ("IBC (Venezuela)", "^IBC"),

    # == Asia-Pacific ==
    ("S&P/ASX 200 (Australia)", "^AXJO"),
    ("All Ordinaries (Australia)", "^AORD"),
    ("SSE Composite (China)", "000001.SS"),
    ("CSI 300 (China)", "000300.SS"),
    ("SZSE Component (China)", "399001.SZ"),
    ("Hang Seng (Hong Kong)", "^HSI"),
    ("Nifty 50 (India)", "^NSEI"),
    ("BSE SENSEX (India)", "^BSESN"),
    ("Jakarta Composite (Indonesia)", "^JKSE"),
    ("TA-125 (Israel)", "^TA125"),
    ("Nikkei 225 (Japan)", "^N225"),
    ("TOPIX (Japan)", "^TPX"),
    ("KLCI (Malaysia)", "^KLSE"),
    ("S&P/NZX 50 (New Zealand)", "^NZ50"),
    ("KSE 100 (Pakistan)", "^KSE"),
    ("PSEi (Philippines)", "^PSEI"),
    ("Tadawul All-Share (Saudi Arabia)", "^TASI.SR"),
    ("Straits Times Index (Singapore)", "^STI"),
    ("KOSPI (South Korea)", "^KS11"),
    ("CSE All-Share (Sri Lanka)", "^CSE"),
    ("TAIEX (Taiwan)", "^TWII"),
    ("SET Index (Thailand)", "^SET.BK"),
    ("VN-Index (Vietnam)", "^VNINDEX"),

    # == Europe, Middle East & Africa ==
    ("ATX (Austria)", "^ATX"),
    ("BEL 20 (Belgium)", "^BFX"),
    ("CROBEX (Croatia)", "^CROBEX"),
    ("PX Index (Czech Republic)", "^PX"),
    ("OMX Copenhagen 25 (Denmark)", "^OMXC25"),
    ("EGX 30 (Egypt)", "^CASE30"),
    ("OMX Helsinki 25 (Finland)", "^OMXH25"),
    ("CAC 40 (France)", "^FCHI"),
    ("DAX (Germany)", "^GDAXI"),
    ("MDAX (Germany)", "MDAXI.DE"),
    ("TecDAX (Germany)", "^TECXP"),
    ("Athex Composite (Greece)", "^ATG"),
    ("BUX (Hungary)", "^BUX"),
    ("ISEQ 20 (Ireland)", "^ISEQ"),
    ("FTSE MIB (Italy)", "^FTSEMIB"),
    ("MASI (Morocco)", "^MSI"),
    ("AEX (Netherlands)", "^AEX"),
    ("OBX (Norway)", "^OBX"),
    ("WIG20 (Poland)", "^WIG20"),
    ("PSI 20 (Portugal)", "^PSI20"),
    ("MOEX (Russia)", "^IMOEX.ME"), # Note: Data may be unreliable/unavailable
    ("JSE Top 40 (South Africa)", "^JTOPI"),
    ("IBEX 35 (Spain)", "^IBEX"),
    ("OMX Stockholm 30 (Sweden)", "^OMX"),
    ("Swiss Market Index (SMI)", "^SSMI"),
    ("BIST 100 (Turkey)", "XU100.IS"),
    ("FTSE 100 (UK)", "^FTSE"),
    ("FTSE 250 (UK)", "^FTMC"),
])

def fetch_index_data(tickers_dict):
    """
    Fetches historical closing prices for a dictionary of index tickers.
    Separates successful fetches from failures and saves them.
    """
    logging.info(f"--- Starting data fetch for {len(tickers_dict)} global indices ---")
    
    successful_dataframes = []
    failed_indices = []

    total_indices = len(tickers_dict)
    for i, (name, ticker) in enumerate(tickers_dict.items()):
        try:
            logging.info(f"Fetching ({i+1}/{total_indices}): {name} ({ticker})")
            data = yf.download(ticker, period=PERIOD, interval=INTERVAL, auto_adjust=True, progress=False)
            
            if data.empty:
                raise ValueError("No data returned from yfinance (likely an invalid or delisted ticker).")
                
            close_price_series = data['Close']
            close_price_series.name = name  # Set the column name to the full index name
            
            successful_dataframes.append(close_price_series)

        except Exception as e:
            logging.warning(f"Could not fetch data for '{name}' ({ticker}).")
            failed_indices.append((name, ticker))

    # --- Process and Save Successful Data ---
    if successful_dataframes:
        final_df = pd.concat(successful_dataframes, axis=1)
        final_df = final_df.reindex(sorted(final_df.columns), axis=1) # Sort columns alphabetically
        final_df.to_csv(OUTPUT_CSV)
        logging.info(f"Successfully fetched data for {len(successful_dataframes)} indices.")
        print(f"\n✅ Data for {len(successful_dataframes)} indices saved to '{OUTPUT_CSV}'")
        print("\n--- Sample of the final data (last 5 days): ---")
        print(final_df.tail())
    else:
        print("\n❌ No data was successfully fetched for any index.")

    # --- Log Failed Indices ---
    if failed_indices:
        logging.info(f"Logging {len(failed_indices)} failed indices to '{FAILED_LOG_FILE}'")
        with open(FAILED_LOG_FILE, 'w') as f:
            f.write("The following indices could not be fetched from Yahoo Finance:\n")
            f.write("-------------------------------------------------------------\n")
            for name, ticker in failed_indices:
                f.write(f"- {name} (tried ticker: {ticker})\n")
        print(f"\n⚠️ Could not fetch {len(failed_indices)} indices. See '{FAILED_LOG_FILE}' for the complete list.")


if __name__ == "__main__":
    fetch_index_data(INDEX_TICKERS)