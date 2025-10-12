#!/usr/bin/env python3
"""
Global Index Analyzer - Ground-Truth Verified Edition

This script uses a multi-strategy scraping engine with a meticulously verified
configuration based on a direct analysis of each page's raw HTML. This represents
the most robust and reliable version for scraping dynamic Wikipedia pages.
"""
import pandas as pd
import yfinance as yf
import time
import requests
import io
import os
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple

# --- The Configuration Hub ---
# FINAL VERSION: Meticulously re-analyzed and verified configuration for all indices.
INDEX_CONFIG = {

    "nikkei225": {
        "name": "Nikkei 225 (Japan)",
        "strategy": "css_class",
        "args": {
            "url": "https://en.wikipedia.org/wiki/Nikkei_225",
            "class_": "wikitable sortable",
            "ticker_column": "Symbol",  # CORRECTED
            "clean_series_fn": lambda s: s.astype(str) + '.T'
        }
    },
    "hangseng": {
        "name": "Hang Seng Index (Hong Kong)",
        "strategy": "css_class", # CORRECTED STRATEGY
        "args": {
            "url": "https://en.wikipedia.org/wiki/Hang_Seng_Index",
            "class_": "wikitable sortable",
            "ticker_column": "Stock Code", # CORRECTED
            "clean_series_fn": lambda s: s.astype(str).str.zfill(4) + '.HK'
        }
    },
    # --- Europe ---
    "ftse100": {
        "name": "FTSE 100 (UK)",
        "strategy": "css_class", # CORRECTED STRATEGY
        "args": {
            "url": "https://en.wikipedia.org/wiki/FTSE_100_Index",
            "class_": "wikitable sortable",
            "ticker_column": "EPIC",
            "clean_series_fn": lambda s: s + '.L'
        }
    },
    "dax": {
        "name": "DAX (Germany)",
        "strategy": "landmark",
        "args": {
            "url": "https://en.wikipedia.org/wiki/DAX",
            "identifier": "DAX companies", # CORRECTED IDENTIFIER
            "ticker_column": "Symbol",
            "clean_series_fn": lambda s: s + '.DE'
        }
    }
}


def _scrape_with_landmark(args: Dict, soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    """Finds a table using the landmark strategy."""
    identifier = args['identifier']
    table_anchor = soup.find(['caption', 'h2', 'h3'], string=re.compile(identifier, re.IGNORECASE))
    if not table_anchor:
        print(f"❌ Error: Landmark '{identifier}' not found.")
        return None
    table_element = table_anchor.find_next('table', {'class': re.compile(r'\bwikitable\b')})
    if not table_element:
        print("❌ Error: Landmark found, but no 'wikitable' table followed.")
        return None
    return pd.read_html(io.StringIO(str(table_element)))[0]

def _scrape_with_css_class(args: Dict, soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    """Finds a table using the CSS class strategy."""
    class_ = args['class_']
    ticker_col = args['ticker_column']
    tables = soup.find_all('table', {'class': class_})
    if not tables:
        print(f"❌ Error: No tables with class '{class_}' found.")
        return None
    for table in tables:
        # Convert table to string, wrap in StringIO and pass to pandas
        df = pd.read_html(io.StringIO(str(table)))[0]
        if ticker_col in df.columns:
            return df # Return the first table that matches
    print(f"❌ Error: Found tables with class '{class_}', but none contained column '{ticker_col}'.")
    return None

def get_constituent_tickers(index_key: str) -> Optional[List[str]]:
    """Main scraping dispatcher. Selects and executes the correct scraping strategy."""
    config = INDEX_CONFIG[index_key]
    name = config['name']
    strategy = config['strategy']
    args = config['args']
    url = args['url']
    ticker_col = args['ticker_column']

    print(f"📋 Step 1: Fetching {name} constituent list using strategy '{strategy}'...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        df = None
        if strategy == 'landmark':
            df = _scrape_with_landmark(args, soup)
        elif strategy == 'css_class':
            df = _scrape_with_css_class(args, soup)
        else:
            print(f"❌ Error: Unknown strategy '{strategy}' for {name}.")
            return None

        if df is None:
            return None

        tickers_series = df[ticker_col]
        cleaned_tickers_series = args['clean_series_fn'](tickers_series)
        tickers = cleaned_tickers_series.tolist()
        print(f"✅ Found {len(tickers)} tickers for {name}.")
        return tickers
        
    except Exception as e:
        print(f"❌ Error: A general error occurred while scraping {name}. {e}")
        return None

# No changes needed for the functions below this line
def get_constituent_data(tickers: List[str], index_name: str) -> List[Dict]:
    """Fetches sector and market cap data for a list of tickers using yfinance."""
    print(f"\n🔎 Step 2: Fetching data for {index_name} constituents...")
    constituent_data = []
    total_tickers = len(tickers)
    for i, ticker_symbol in enumerate(tickers, 1):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            sector = info.get('sector', 'N/A')
            market_cap = info.get('marketCap', 0)
            if sector != 'N/A' and market_cap > 0:
                constituent_data.append({'Ticker': ticker_symbol, 'Sector': sector, 'MarketCap': market_cap})
                print(f"   ({i}/{total_tickers}) Fetching {ticker_symbol:<15}...", end='\r')
            else:
                print(f"   ({i}/{total_tickers}) ⚠️  Skipping {ticker_symbol:<15} - Missing data.", " "*25)
            time.sleep(0.05)
        except Exception:
            print(f"   ({i}/{total_tickers}) ❌ Error fetching data for {ticker_symbol:<15}.", " "*25)
    print("\n✅ Fetching complete for this index.")
    return constituent_data

def analyze_and_display_results(constituent_data: List[Dict], index_name: str) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    """Analyzes data, prints results, and returns the analysis as pandas DataFrames."""
    if not constituent_data:
        print("\nNo data to analyze for this index.")
        return None
    print(f"\n📊 Step 3: Analyzing results for {index_name}...")
    df = pd.DataFrame(constituent_data)
    sector_counts = df['Sector'].value_counts().reset_index()
    sector_counts.columns = ['Sector', 'Count']
    sector_counts['Percentage'] = (sector_counts['Count'] / len(df)) * 100
    print("\n--- Sector Breakdown (by Company Count) ---")
    print(sector_counts.to_string(index=False))
    total_market_cap = df['MarketCap'].sum()
    sector_weights = df.groupby('Sector')['MarketCap'].sum().reset_index()
    sector_weights = sector_weights.sort_values(by='MarketCap', ascending=False)
    sector_weights['Weight (%)'] = (sector_weights['MarketCap'] / total_market_cap) * 100
    print("\n--- Sector Breakdown (by Market-Cap Weight) ---")
    print(sector_weights[['Sector', 'Weight (%)']].to_string(index=False))
    try:
        currency = yf.Ticker(constituent_data[0]['Ticker']).info.get('currency', 'N/A')
        print(f"\nTotal Market Cap Analyzed: {currency} {total_market_cap:,.0f}")
    except:
        print(f"\nTotal Market Cap Analyzed: {total_market_cap:,.0f}")
    return sector_counts, sector_weights


def main():
    """Main function to run the batch analysis and save results to CSV files."""
    print("--- Starting Global Index Analysis ---")
    start_time = time.time()
    
    all_results = {}
    output_dir = "global_index_reports"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    print(f"Reports will be saved in the '{output_dir}' folder.")

    for index_key in INDEX_CONFIG:
        print("\n" + "="*70)
        print(f"Analyzing Index: {INDEX_CONFIG[index_key]['name']}")
        print("="*70)
        
        tickers = get_constituent_tickers(index_key)
        if tickers:
            data = get_constituent_data(tickers, INDEX_CONFIG[index_key]['name'])
            if data:
                analysis_result = analyze_and_display_results(data, INDEX_CONFIG[index_key]['name'])
                if analysis_result:
                    count_df, weight_df = analysis_result
                    all_results[INDEX_CONFIG[index_key]['name']] = {
                        "count_breakdown": count_df,
                        "weight_breakdown": weight_df
                    }

    print("\n" + "="*70)
    print("Saving analysis results to CSV files...")
    for index_name, dfs in all_results.items():
        clean_name = index_name.replace(" ", "_").replace("(", "").replace(")", "")
        
        count_filename = os.path.join(output_dir, f"{clean_name}_count_breakdown.csv")
        dfs["count_breakdown"].to_csv(count_filename, index=False)
        print(f"  ✅ Saved: {count_filename}")

        weight_filename = os.path.join(output_dir, f"{clean_name}_weight_breakdown.csv")
        dfs["weight_breakdown"].to_csv(weight_filename, index=False)
        print(f"  ✅ Saved: {weight_filename}")

    end_time = time.time()
    total_minutes = (end_time - start_time) / 60
    print("\n" + "="*70)
    print("Global batch analysis and file saving complete.")
    print(f"Total execution time: {total_minutes:.2f} minutes.")
    print("="*70)

if __name__ == "__main__":
    main()