#!/usr/bin/env python3
"""
Global Index Analyzer - DIAGNOSTIC MODE

This script inspects the structure of each Wikipedia page to identify the
correct landmarks (headings) and table columns. It does NOT fetch financial data.
Its purpose is to gather the necessary information to build a robust configuration.
"""
import pandas as pd
import requests
import io
import re
from bs4 import BeautifulSoup
# In your diagnostic script, replace the INDEX_CONFIG with this complete version:

# In your diagnostic script, use this focused INDEX_CONFIG:

# In your diagnostic script, use this new focused INDEX_CONFIG:

INDEX_CONFIG = {
    "mdax": {"name": "MDAX (Germany)", "url": "https://en.wikipedia.org/wiki/MDAX"},
    "sdax": {"name": "SDAX (Germany)", "url": "https://en.wikipedia.org/wiki/SDAX"},
    "omxi10": {"name": "OMX Iceland 10 (Iceland)", "url": "https://en.wikipedia.org/wiki/OMX_Iceland_10"}
}
def diagnose_page_structure(index_name: str, url: str):
    """
    Fetches a Wikipedia page and prints a report of its structure, including
    headings and the columns of any data tables found.
    """
    print(f"\n{'='*80}")
    print(f"--- DIAGNOSING: {index_name} ---")
    print(f"URL: {url}")
    print(f"{'='*80}")

    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
    except requests.RequestException as e:
        print(f"❌ FAILED TO FETCH URL: {e}")
        return

    # --- Step 1: Find all potential landmarks (headings) ---
    landmarks = soup.find_all(['h2', 'h3'])
    print("\n[ Potential Landmarks (Headings Found) ]")
    if landmarks:
        for mark in landmarks:
            # The actual text is usually inside a span with class 'mw-headline'
            headline = mark.find('span', class_='mw-headline')
            if headline:
                print(f"  -> {headline.get_text(strip=True)}")
    else:
        print("  - No <h2> or <h3> headings found.")

    # --- Step 2: Find all data tables and list their columns ---
    tables = soup.find_all('table', {'class': 'wikitable'})
    print("\n[ Analysis of Found Wikitables ]")
    if not tables:
        print("  - No tables with class 'wikitable' found on this page.")
        return

    for i, table in enumerate(tables):
        print(f"\n--- Table #{i+1} ---")
        try:
            # Use pandas to easily parse the table and get columns
            df_list = pd.read_html(io.StringIO(str(table)))
            if not df_list:
                print("  - Pandas could not parse this table.")
                continue
            
            df = df_list[0]
            
            # Clean up multi-level column headers if they exist
            if isinstance(df.columns, pd.MultiIndex):
                # Join the levels of the multi-index with an underscore
                df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
            
            print(f"  ✅ Columns: {df.columns.tolist()}")

        except Exception as e:
            print(f"  - Could not parse this table. Error: {e}")


def main():
    """Main function to run the diagnostic tool for every index."""
    print("--- Starting Wikipedia Page Structure Analysis ---")
    print("This tool will report the structure of each page for configuration.")
    
    for config in INDEX_CONFIG.values():
        diagnose_page_structure(config['name'], config['url'])
        
    print(f"\n{'='*80}")
    print("--- DIAGNOSIS COMPLETE ---")
    print("Please copy the entire output above and provide it for analysis.")
    print("="*80)


if __name__ == "__main__":
    main()