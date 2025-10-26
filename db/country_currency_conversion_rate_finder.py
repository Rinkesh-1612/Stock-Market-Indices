import os
import requests
# --- CHANGE 1: Import the new library ---
from countryinfo import CountryInfo
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.orm import sessionmaker, declarative_base
import csv

# --- Database Configuration ---
# Make sure to install our new libraries:
# pip install requests sqlalchemy mysqlclient countryinfo
engine = create_engine("mysql://harsh:hva123@localhost/stock_data")

# --- SQLAlchemy Setup ---
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

# --- 1. Define Our Two New Models ---

class CountryCurrencyMap(Base):
    """Maps a country name (from your tables) to its currency code."""
    __tablename__ = 'country_currency_map'
    country_name = Column(String(255), primary_key=True)
    currency_code = Column(String(3), index=True)

class CurrencyRate(Base):
    """Stores the conversion rate for a single currency to USD."""
    __tablename__ = 'currency_rates'
    currency_code = Column(String(3), primary_key=True)
    rate_to_usd = Column(Float, nullable=False)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# --- 2. The Main Function ---

def update_currency_data():
    """
    Fetches all unique countries, maps them to currencies,
    and gets the latest conversion rates.
    """
    api_key_path = 'api_keys.txt'
    try:
        with open(api_key_path, 'r') as file:
            api_key = file.read().strip()
            if not api_key:
                print(f"Error: {api_key_path} is empty.")
                return
        API_KEY = api_key
        print(f"Using API Key from {api_key_path}")
    except FileNotFoundError:
        print(f"Error: API key file not found at '{api_key_path}'")
        return
    except Exception as e:
        print(f"Error reading API key file: {e}")
        return

    API_URL = f'https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD'
    
    # --- CHANGE 2: No longer need country_converter (coco) ---
    # cc = coco.CountryConverter() 
    
    try:
        # 3. Get all unique countries
        sql_query = """
        SELECT DISTINCT country FROM index_count_breakdown
        UNION
        SELECT DISTINCT country FROM index_weight_breakdown;
        """
        result = session.execute(text(sql_query)).all()
        country_names = [row[0] for row in result if row[0] is not None]
        
        if not country_names:
            print("No countries found in index tables. Exiting.")
            return
            
        print(f"Found {len(country_names)} unique countries. Mapping to currencies...")

        # 4. Map countries to currencies
        processed_codes = set()
        for country in country_names:
            if session.get(CountryCurrencyMap, country):
                continue
            
            # --- CHANGE 3: Use countryinfo to get the currency ---
            try:
                # This library can handle most country name variations
                country_data = CountryInfo(country)
                # currencies() returns a list, e.g., ['USD']. We take the first one.
                currency_code = country_data.currencies()[0]
                
                if currency_code:
                    new_map = CountryCurrencyMap(
                        country_name=country, 
                        currency_code=currency_code
                    )
                    session.add(new_map)
                    processed_codes.add(currency_code)
                    print(f"  -> Mapped '{country}' to '{currency_code}'")
                else:
                    print(f"  -> WARNING: Could not find currency for '{country}'.")
            
            except Exception as e:
                # Handle cases where countryinfo can't find the country
                print(f"  -> ERROR: Could not process country '{country}'. Reason: {e}")
        
        session.commit()
        print("Country-to-currency mapping complete.")

        # 5. Get latest conversion rates
        if not processed_codes:
            print("No new currency codes to process. Checking all known codes.")
            all_codes_q = session.query(CountryCurrencyMap.currency_code).distinct()
            processed_codes = {code[0] for code in all_codes_q}
            if not processed_codes:
                print("No currency codes found in map table either. Exiting.")
                return

        print(f"\nFetching exchange rates for {len(processed_codes)} currencies...")
        
        response = requests.get(API_URL)
        response.raise_for_status() 
        data = response.json()

        if data.get('result') != 'success':
            print(f"Error from API: {data.get('error-type')}")
            return
            
        rates_from_api = data['conversion_rates']

        # 6. Populate/Update the 'currency_rates' table
        for code in processed_codes:
            rate = rates_from_api.get(code)
            
            if not rate:
                print(f"  -> WARNING: No rate returned from API for '{code}'.")
                continue
                
            existing_rate = session.get(CurrencyRate, code)
            
            if existing_rate:
                existing_rate.rate_to_usd = rate
            else:
                new_rate = CurrencyRate(
                    currency_code=code,
                    rate_to_usd=rate
                )
                session.add(new_rate)
            
            print(f"  -> Stored rate for '{code}': {rate}")

        session.commit()
        print("\nSuccessfully updated all currency rates.")

    except requests.exceptions.HTTPError as e:
        session.rollback()
        print(f"HTTP Error from API: {e}")
        if e.response.status_code == 401:
            print("-> This is an 'Unauthorized' error. Check your API key.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()
        print("Database session closed.")

# --- 3. Run the Script ---
if __name__ == "__main__":
    try:
        Base.metadata.create_all(engine)
        print("Tables 'country_currency_map' and 'currency_rates' verified.")
        update_currency_data()
    
    except Exception as e:
        print(f"A critical error occurred connecting to the database or creating tables: {e}")
        print("Please check your database connection string and ensure MySQL is running.")