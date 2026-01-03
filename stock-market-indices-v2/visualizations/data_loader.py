import pandas as pd
import os
import zipfile
import gc
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    _instance = None
    _df_prices = None
    _df_indices = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DataLoader, cls).__new__(cls)
        return cls._instance

    def get_data_dir(self):
        # Assuming this file is in stock-market-indices-v2/visualizations/
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'final')

    def load_indices(self):
        if self._df_indices is not None:
            return self._df_indices
        
        try:
            data_dir = self.get_data_dir()
            file_path = os.path.join(data_dir, "data_cleaned", "master_indices_list.csv")
            self._df_indices = pd.read_csv(file_path)
            logger.info("Loaded master indices list.")
        except Exception as e:
            logger.error(f"Error loading indices: {e}")
            self._df_indices = pd.DataFrame()
        
        return self._df_indices

    def load_prices(self):
        if self._df_prices is not None:
            return self._df_prices

        try:
            logger.info("Loading price data...")
            data_dir = self.get_data_dir()
            prices_path = os.path.join(data_dir, "final_all_historical_prices_parquet.zip")
            
            with zipfile.ZipFile(prices_path, 'r') as zf:
                parquet_filename = [f for f in zf.namelist() if f.endswith('.parquet')][0]
                with zf.open(parquet_filename) as pf:
                    # Load directly
                    df = pd.read_parquet(pf, engine='pyarrow')
            
            # Optimization: Convert to float32 to save memory
            for col in df.select_dtypes(include=['float64']).columns:
                df[col] = df[col].astype('float32')

            # Ensure index is datetime
            if 'Date' in df.columns:
                df = df.set_index('Date')
            df.index = pd.to_datetime(df.index)

            self._df_prices = df
            logger.info(f"Loaded price data. Shape: {df.shape}, Memory: {df.memory_usage().sum() / 1024**2:.2f} MB")
            
            # Force garbage collection
            gc.collect()

        except Exception as e:
            logger.error(f"Error loading price data: {e}")
            self._df_prices = pd.DataFrame()

        return self._df_prices

# Global instance
global_data_loader = DataLoader()
