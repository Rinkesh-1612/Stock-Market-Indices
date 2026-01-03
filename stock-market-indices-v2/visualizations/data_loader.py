import pandas as pd
import os
import zipfile
import gc
import shutil
import logging
import tempfile
import pyarrow.parquet as pq

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

        temp_dir = tempfile.mkdtemp()
        temp_parquet_path = os.path.join(temp_dir, 'temp_prices.parquet')

        try:
            logger.info("Starting optimized batched data loading...")
            data_dir = self.get_data_dir()
            zip_path = os.path.join(data_dir, "final_all_historical_prices_parquet.zip")
            
            # 1. Extract to temp file (Streamed to avoid memory spike)
            logger.info("Extracting parquet file...")
            with zipfile.ZipFile(zip_path, 'r') as zf:
                parquet_filename = [f for f in zf.namelist() if f.endswith('.parquet')][0]
                with zf.open(parquet_filename) as source, open(temp_parquet_path, "wb") as target:
                    shutil.copyfileobj(source, target)
            
            # Force GC after extraction
            gc.collect()

            # 2. Inspect columns
            pf = pq.ParquetFile(temp_parquet_path)
            all_columns = pf.schema.names
            
            # 3. Identify Index vs Stock columns
            df_indices = self.load_indices()
            index_tickers = set(df_indices['Ticker'].values)
            
            # 'Date' is essential for alignment
            cols_to_load_full = [c for c in all_columns if c in index_tickers or c == 'Date']
            cols_to_load_partial = [c for c in all_columns if c not in index_tickers and c != 'Date']

            # 4. Load Full History for Indices (Required for Comparator, Growth, Correlation)
            logger.info(f"Loading full history for {len(cols_to_load_full)} index columns...")
            df_full = pd.read_parquet(temp_parquet_path, columns=cols_to_load_full)
            if 'Date' in df_full.columns:
                df_full = df_full.set_index('Date')
            df_full.index = pd.to_datetime(df_full.index)
            
            # Optimize Indices
            for col in df_full.select_dtypes(include=['float64']).columns:
                df_full[col] = df_full[col].astype('float32')

            # 5. Load Partial History for Stocks (Batched, Required for Treemap)
            # We only need the last few rows for the Treemap (current price, 1d, 1w, 1m changes)
            # 30 rows is plenty safe.
            BATCH_SIZE = 500
            partial_dfs = []
            
            logger.info(f"Loading partial history (last 30 days) for {len(cols_to_load_partial)} stock columns...")
            for i in range(0, len(cols_to_load_partial), BATCH_SIZE):
                batch_cols = cols_to_load_partial[i : i + BATCH_SIZE]
                # Read Date + Batch
                df_batch = pd.read_parquet(temp_parquet_path, columns=['Date'] + batch_cols)
                
                # Keep only last 30 days
                df_batch = df_batch.tail(30)
                
                if 'Date' in df_batch.columns:
                    df_batch = df_batch.set_index('Date')
                df_batch.index = pd.to_datetime(df_batch.index)
                
                # Optimize
                for col in df_batch.select_dtypes(include=['float64']).columns:
                    df_batch[col] = df_batch[col].astype('float32')
                
                partial_dfs.append(df_batch)
                gc.collect()

            # 6. Combine
            logger.info("Merging dataframes...")
            self._df_prices = pd.concat([df_full] + partial_dfs, axis=1)
            
            # Cleanup intermediate variables to free memory immediately
            del df_full
            del partial_dfs
            gc.collect()
            
            logger.info(f"Loaded. Shape: {self._df_prices.shape}, Memory: {self._df_prices.memory_usage().sum() / 1024**2:.2f} MB")

        except Exception as e:
            logger.error(f"Error loading price data: {e}")
            self._df_prices = pd.DataFrame()
        
        finally:
            # Cleanup temp file
            try:
                if os.path.exists(temp_parquet_path):
                    os.remove(temp_parquet_path)
                if os.path.exists(temp_dir):
                    os.rmdir(temp_dir)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp files: {e}")
            
            gc.collect()

        return self._df_prices

# Global instance
global_data_loader = DataLoader()
