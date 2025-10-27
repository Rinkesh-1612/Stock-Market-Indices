import yfinance as yf
import pandas as pd
import logging
from collections import OrderedDict

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
PERIOD = "3y"
INTERVAL = "1d"
OUTPUT_CSV = "global_indices_master_3y_daily_with_dowjone.csv"
FAILED_LOG_FILE = "failed_to_fetch_indices.txt"

# --- The Comprehensive, Manually Curated Dictionary of World Indices (v4 - Final & Most Robust) ---
# Replaced the unreliable ^GDOW with the more stable Dow Jones Global Index ^DJW.
# Removed KSE (Pakistan) due to persistent poor data quality.
INDEX_TICKERS = OrderedDict([
    # == Global Indices ==
    ("Dow Jones Global Titans 50", "^DJGT"),
    ("FTSE All-World", "ACWI"),        # ETF Proxy (iShares MSCI ACWI) is far more reliable
    ("S&P Global 100", "IOO"),         # Using iShares ETF for reliability
    ("S&P Global 1200", "^SPG1200"),
    ("Dow Jones Global Index", "^DJW"),# <-- CHANGED: Replaced unreliable ^GDOW with stable ^DJW
    ("MSCI World", "URTH"),            # ETF proxy for developed markets
    ("MSCI EAFE", "EFA"),              # ETF proxy for Europe, Australasia, Far East

    # == Regional Indices ==
    ("S&P Asia 50", "AIA"),            # ETF Proxy (iShares Asia 50)
    ("EURO STOXX 50", "^STOXX50E"),
    ("STOXX Europe 600", "^STOXX"),
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
    ("COLCAP (Colombia)", "ICOL"),      # ETF Proxy (iShares MSCI Colombia)
    ("IPC (Mexico)", "^MXX"),
    ("S&P/BVL Peru General", "EPU"),   # ETF Proxy (iShares MSCI Peru)

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
    ("TA-125 (Israel)", "^TA125.TA"),
    ("Nikkei 225 (Japan)", "^N225"),
    ("TOPIX (Japan)", "EWJ"),          # ETF Proxy (iShares MSCI Japan)
    ("KLCI (Malaysia)", "^KLSE"),
    ("S&P/NZX 50 (New Zealand)", "^NZ50"),
    ("PSEi (Philippines)", "^PSI"),    # Correct Ticker
    ("Tadawul All-Share (Saudi Arabia)", "^TASI.SR"),
    ("Straits Times Index (Singapore)", "^STI"),
    ("KOSPI (South Korea)", "^KS11"),
    ("TAIEX (Taiwan)", "^TWII"),
    ("SET Index (Thailand)", "^SET.BK"),
    ("VN-Index (Vietnam)", "^VNI"),    # Correct Ticker

    # == Europe, Middle East & Africa ==
    ("ATX (Austria)", "^ATX"),
    ("BEL 20 (Belgium)", "^BFX"),
    ("OMX Copenhagen 25 (Denmark)", "^OMXC25"),
    ("EGX 30 (Egypt)", "^CASE30"),
    ("OMX Helsinki 25 (Finland)", "^OMXH25"),
    ("CAC 40 (France)", "^FCHI"),
    ("DAX (Germany)", "^GDAXI"),
    ("MDAX (Germany)", "^MDAXI"),
    ("TecDAX (Germany)", "^TECDAX"),
    ("Athex Composite (Greece)", "GREK"), # ETF Proxy (Global X MSCI Greece)
    ("ISEQ 20 (Ireland)", "^ISEQ"),
    ("FTSE MIB (Italy)", "EWI"),       # ETF Proxy (iShares MSCI Italy)
    ("AEX (Netherlands)", "^AEX"),
    ("OBX (Norway)", "NORW"),          # ETF Proxy (Global X MSCI Norway)
    ("WIG20 (Poland)", "EPOL"),        # ETF Proxy (iShares MSCI Poland)
    ("PSI 20 (Portugal)", "PGAL"),     # ETF Proxy (Global X MSCI Portugal)
    ("MOEX (Russia)", "IMOEX.ME"),     # NOTE: Data stops around Feb 2022
    ("JSE Top 40 (South Africa)", "^J201.JO"),
    ("IBEX 35 (Spain)", "^IBEX"),
    ("OMX Stockholm 30 (Sweden)", "^OMX"),
    ("Swiss Market Index (SMI)", "^SSMI"),
    ("BIST 100 (Turkey)", "XU100.IS"),
    ("FTSE 100 (UK)", "^FTSE"),
    ("FTSE 250 (UK)", "^FTMC"),
])
import time # Import the time library for a small delay

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
            data = yf.Ticker(ticker).history(period=PERIOD, interval=INTERVAL, auto_adjust=True)

            if data.empty:
                raise ValueError("No data returned from yfinance (likely an invalid or delisted ticker).")

            close_price_series = data['Close']
            
            # --- THIS IS THE FIX ---
            # The index from yfinance is timezone-aware. To align different markets
            # (e.g., Tokyo close vs. New York close) on the same calendar day, we
            # must normalize the index by removing the time and timezone info.
            close_price_series.index = pd.to_datetime(close_price_series.index.date)
            
            close_price_series.name = name
            successful_dataframes.append(close_price_series)
            
            # Add a small delay to be polite to Yahoo's servers and avoid getting blocked
            time.sleep(0.1) 

        except Exception as e:
            logging.warning(f"Could not fetch data for '{name}' ({ticker}). Reason: {e}")
            failed_indices.append((name, ticker))

    # --- Process and Save Successful Data ---
    if successful_dataframes:
        # Use 'outer' join to handle different market holidays correctly.
        # This ensures that if one market is open while another is closed,
        # the date is kept and the closed market shows NaN.
        final_df = pd.concat(successful_dataframes, axis=1, join='outer')
        
        final_df.index.name = 'Date'
        final_df = final_df.sort_index() # Sort by date to ensure chronological order
        final_df = final_df.reindex(sorted(final_df.columns), axis=1) # Sort columns alphabetically
        
        # Optional: Forward-fill NaN values for markets that were closed on holidays
        # final_df = final_df.fillna(method='ffill')
        
        final_df.to_csv(OUTPUT_CSV)
        logging.info(f"Successfully fetched data for {len(successful_dataframes)} indices.")
        print(f"\n Data for {len(successful_dataframes)} indices saved to '{OUTPUT_CSV}'")
        print("\n--- Sample of the final data (last 5 days): ---")
        print(final_df.tail())
    else:
        print("\nNo data was successfully fetched for any index.")

    # --- Log Failed Indices ---
    if failed_indices:
        logging.info(f"Logging {len(failed_indices)} failed indices to '{FAILED_LOG_FILE}'")
        with open(FAILED_LOG_FILE, 'w') as f:
            f.write("The following indices could not be fetched from Yahoo Finance:\n")
            f.write("-------------------------------------------------------------\n")
            for name, ticker in failed_indices:
                f.write(f"- {name} (tried ticker: {ticker})\n")
        print(f"\n Could not fetch {len(failed_indices)} indices. See '{FAILED_LOG_FILE}' for the complete list.")

if __name__ == "__main__":
    fetch_index_data(INDEX_TICKERS)