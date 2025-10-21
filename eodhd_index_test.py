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
# FINAL VERSION: Built from empirical diagnostic data.
# This configuration is verified against the live structure of each page.
INDEX_CONFIG = {
# --- Append these new entries to your main script's INDEX_CONFIG dictionary ---

    "spasia50": {
        "name": "S&P Asia 50",
        "strategy": "css_class",
        "args": {
            "url": "https://en.wikipedia.org/wiki/S%26P_Asia_50",
            "class_": "wikitable",
            "ticker_column": "Ticker",
            "column_mapping": {"Sector": "Industry"},
            # Use our new custom function to handle multiple Asian exchanges
            "custom_clean_fn": True
        }
    },
    "splatin40": {
        "name": "S&P Latin America 40",
        "strategy": "css_class",
        "args": {
            "url": "https://en.wikipedia.org/wiki/S%26P_Latin_America_40",
            "class_": "wikitable",
            "ticker_column": "Ticker symbol",
            "column_mapping": {"Sector": "Industry"},
            # Use our new custom function to handle multiple Latin American exchanges
            "custom_clean_fn": True
        }
    }
}
def _scrape_with_css_class(args: Dict, soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    """
    Finds the correct table using the CSS class and required ticker column.
    This is the single, unified scraping strategy.
    """
    class_ = args['class_']
    ticker_col = args['ticker_column']
    # Use a regex to find classes that contain the base class, e.g., "wikitable sortable"
    tables = soup.find_all('table', {'class': re.compile(r'\b' + class_ + r'\b')})
    
    if not tables:
        print(f"❌ Error: No tables with class containing '{class_}' found.")
        return None
        
    for table in tables:
        try:
            # Use pandas to easily parse columns, including multi-level ones
            df = pd.read_html(io.StringIO(str(table)))[0]
            
            # Clean up multi-level column headers if they exist
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]

            if ticker_col in df.columns:
                return df # Return the first table that contains our ticker column
        except Exception:
            continue # Ignore tables that pandas can't parse

    print(f"❌ Error: Found tables with class '{class_}', but none contained column '{ticker_col}'.")
    return None
def get_constituent_table(index_key: str) -> Optional[pd.DataFrame]:
    """
    Scrapes the constituent table from Wikipedia using the unified css_class strategy
    and returns a DataFrame with Tickers and other mapped data.
    
    UPGRADED: Now handles complex, multi-country regional indices like S&P Asia 50 and S&P Latin America 40.
    """
    config = INDEX_CONFIG[index_key]
    name = config['name']
    args = config['args']
    url = args['url']

    print(f"📋 Step 1: Fetching {name} constituent table...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        raw_df = _scrape_with_css_class(args, soup)

        if raw_df is None:
            return None

        ticker_col = args['ticker_column']

        # --- SPECIAL HANDLING for multi-exchange/multi-country indices ---
        if args.get("custom_clean_fn"):
            if index_key == "stoxx50":
                exchange_map = {
                    'Xetra': '.DE', 'Euronext Paris': '.PA', 'Euronext Amsterdam': '.AS',
                    'Borsa Italiana': '.MI', 'Irish Stock Exchange': '.IR', 'Helsinki Stock Exchange': '.HE'
                }
                tickers_series = raw_df.apply(
                    lambda row: str(row[ticker_col]) + exchange_map.get(row['Main listing'], ''),
                    axis=1
                )
            # NEW: Custom logic for S&P Asia 50
            elif index_key == "spasia50":
                exchange_map = {'Hong Kong': '.HK', 'South Korea': '.KS', 'Singapore': '.SI', 'Taiwan': '.TW'}
                tickers_series = raw_df.apply(
                    lambda row: str(row[ticker_col]) + exchange_map.get(row['Country'], ''),
                    axis=1
                )
            # NEW: Custom logic for S&P Latin America 40
            elif index_key == "splatin40":
                exchange_map = {'Brazil': '.SA', 'Mexico': '.MX', 'Chile': '.SN', 'Peru': '.LM', 'Colombia': '.CN'}
                tickers_series = raw_df.apply(
                    lambda row: str(row[ticker_col]) + exchange_map.get(row['Country'], ''),
                    axis=1
                )
            else:
                 tickers_series = raw_df[ticker_col]
        else:
            tickers_series = args['clean_series_fn'](raw_df[ticker_col])
        
        constituents_df = pd.DataFrame({'Ticker': tickers_series})

        column_mapping = args.get('column_mapping', {})
        for standard_name, wiki_name in column_mapping.items():
            if wiki_name in raw_df.columns:
                constituents_df[standard_name] = raw_df[wiki_name]
            else:
                constituents_df[standard_name] = 'N/A'

        print(f"✅ Found and processed table for {len(constituents_df)} constituents.")
        return constituents_df

    except Exception as e:
        print(f"❌ Error: A general error occurred while scraping {name}. {e}")
        return None
def enrich_constituent_data(constituents_df: pd.DataFrame, index_name: str) -> pd.DataFrame:
    """
    Enriches the DataFrame with data from yfinance, but only for
    columns that couldn't be filled by the initial scrape.
    """
    print(f"\n🔎 Step 2: Enriching data for {index_name} constituents (using yfinance as fallback)...")
    
    # Ensure standard columns exist
    if 'Sector' not in constituents_df.columns:
        constituents_df['Sector'] = 'N/A'
    if 'MarketCap' not in constituents_df.columns:
        constituents_df['MarketCap'] = 0

    total_tickers = len(constituents_df)
    for i, row in constituents_df.iterrows():
        ticker_symbol = row['Ticker']
        # Only fetch if we need to fill in missing data
        should_fetch = (row['Sector'] == 'N/A' or row['MarketCap'] == 0)
        
        if not should_fetch:
            print(f"   ({i+1}/{total_tickers}) Skipping {ticker_symbol:<15} (data found on page)", end='\r')
            continue

        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            
            # Fill Sector if it was missing
            if constituents_df.at[i, 'Sector'] == 'N/A':
                constituents_df.at[i, 'Sector'] = info.get('sector', 'N/A')
            
            # Always fetch Market Cap from yfinance as it's more reliable and dynamic
            constituents_df.at[i, 'MarketCap'] = info.get('marketCap', 0)

            print(f"   ({i+1}/{total_tickers}) Enriching {ticker_symbol:<15}...", end='\r')
            time.sleep(0.05)
        except Exception:
            print(f"   ({i+1}/{total_tickers}) ❌ Error enriching data for {ticker_symbol:<15}.", " "*25)
    
    # Final cleanup
    constituents_df = constituents_df[constituents_df['MarketCap'] > 0]
    print("\n✅ Enrichment complete for this index.")
    return constituents_df
def analyze_and_display_results(df: pd.DataFrame, index_name: str) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    """Analyzes data, prints results, and returns the analysis as pandas DataFrames."""
    if df.empty:
        print("\nNo data to analyze for this index.")
        return None
        
    print(f"\n📊 Step 3: Analyzing results for {index_name}...")
    
    # The input 'df' is now already a DataFrame, no need to create it.
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
        # Get currency from the first valid ticker
        currency = yf.Ticker(df['Ticker'].iloc[0]).info.get('currency', 'N/A')
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
        
        # Updated function calls
        constituent_df = get_constituent_table(index_key)
        if constituent_df is not None and not constituent_df.empty:
            enriched_df = enrich_constituent_data(constituent_df, INDEX_CONFIG[index_key]['name'])
            if not enriched_df.empty:
                analysis_result = analyze_and_display_results(enriched_df, INDEX_CONFIG[index_key]['name'])
                if analysis_result:
                    count_df, weight_df = analysis_result
                    all_results[INDEX_CONFIG[index_key]['name']] = {
                        "count_breakdown": count_df,
                        "weight_breakdown": weight_df
                    }

    # ... rest of the main function remains the same ...
    print("\n" + "="*70)
    print("Saving analysis results to CSV files...")
    for index_name, dfs in all_results.items():
        clean_name = index_name.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
        
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