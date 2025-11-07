import pandas as pd
import re
import sys
from pathlib import Path  # We'll use pathlib to handle files and directories

# -----------------------------------------------------------------------------
# 1. CORE CLEANING LOGIC (Unchanged)
# -----------------------------------------------------------------------------

def clean_name(name):
    """Applies all cleaning rules to a single company name."""
    
    # Ensure the name is a string, otherwise return it as is (e.g., handles NaN)
    if not isinstance(name, str):
        return name
    
    # Rule 1: Remove company name enclosed in brackets at the end
    cleaned_name = re.sub(r'\s*[\(\[].*?[\)\]]\s*$', '', name)
    
    # Rule 2: Remove a trailing single letter preceded by whitespace
    cleaned_name = re.sub(r'\s+[A-Za-z]$', '', cleaned_name)
    
    # Rule 3: Remove trailing periods and spaces
    cleaned_name = cleaned_name.rstrip(' .')
    
    # Rule 4: Normalize all internal whitespace
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
    
    # Finally, strip any leading/trailing whitespace left over
    cleaned_name = cleaned_name.strip()
    
    return cleaned_name

def clean_company_csv(input_path, col_index, output_path):
    """
    Cleans a specific column in a CSV file and saves it to a new file.
    
    :param input_path: Path to the original CSV file.
    :param col_index: The integer index of the column to clean (0 for first col, 1 for second, etc.).
    :param output_path: Path to save the cleaned CSV file.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(input_path)
        
        # Check if the column index is valid
        if col_index < 0 or col_index >= len(df.columns):
            print(f"Error: Column index {col_index} is out of bounds for file {input_path}.")
            print(f"File has {len(df.columns)} columns (indices 0 to {len(df.columns) - 1}).")
            return

        # Get the name of the column at the specified index
        column_to_clean = df.columns[col_index]
        
        print(f"Cleaning column '{column_to_clean}' (index {col_index})...")
        
        # Apply the cleaning function to every row in that column
        df[column_to_clean] = df[column_to_clean].apply(clean_name)
        
        # Save the cleaned DataFrame to a new CSV
        df.to_csv(output_path, index=False)
        
        print(f"Successfully cleaned file and saved to '{output_path}'")

    except FileNotFoundError:
        print(f"Error: The file '{input_path}' was not found.")
    except Exception as e:
        print(f"An error occurred while processing {input_path}: {e}")

# -----------------------------------------------------------------------------
# 2. NEW BATCH PROCESSING LOGIC
# -----------------------------------------------------------------------------

def main():
    """
    Main function to find and process all specified CSV files.
    """
    # --- Configuration ---
    
    # The column index to clean in all files
    COLUMN_TO_CLEAN = 0  # 0 is the first column, 1 is the second, etc.
    
    # 1. A directory where you want to clean ALL .csv files
    INPUT_DIRECTORY = "/home/harshvardhan/dcv4/Stock-Market-Indices/stock-market-indices-v2/data/constituent_lists"
    
    # 2. A list of other, specific file paths to clean
    SPECIFIC_FILES = [
        "/home/harshvardhan/dcv4/Stock-Market-Indices/stock-market-indices-v2/data/final/constituent_verification_summary.csv",
        "/home/harshvardhan/dcv4/Stock-Market-Indices/stock-market-indices-v2/data/final/master_indices_list.csv"
    ]
    
    # 3. A single directory where ALL cleaned files will be saved
    OUTPUT_DIRECTORY = "/home/harshvardhan/dcv4/Stock-Market-Indices/stock-market-indices-v2/data/final/data_cleaned"
    
    # ---------------------
    
    # Create a set to store all unique file paths (avoids processing duplicates)
    files_to_process = set()
    
    # --- Step 1: Add files from the input directory ---
    input_dir = Path(INPUT_DIRECTORY)
    if input_dir.is_dir():
        print(f"Scanning directory: {input_dir}")
        # Use .glob('*.csv') to find only CSV files
        for file_path in input_dir.glob('*.csv'):
            files_to_process.add(file_path)
    else:
        print(f"Warning: Input directory '{INPUT_DIRECTORY}' not found. Skipping.")

    # --- Step 2: Add the specific files ---
    for file_path_str in SPECIFIC_FILES:
        file_path = Path(file_path_str)
        if file_path.exists():
            files_to_process.add(file_path)
        else:
            print(f"Warning: Specific file '{file_path_str}' not found. Skipping.")
            
    if not files_to_process:
        print("No valid files found to process.")
        return

    # --- Step 3: Create the output directory (if it doesn't exist) ---
    output_dir = Path(OUTPUT_DIRECTORY)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nAll cleaned files will be saved in: {output_dir.resolve()}")
    
    # --- Step 4: Process each file ---
    print(f"\nFound {len(files_to_process)} unique files to clean.")
    
    for input_path in files_to_process:
        # Generate a new output path: <OUTPUT_DIRECTORY>/<original_filename.csv>
        output_path = output_dir / input_path.name
        
        print(f"\n--- Processing: {input_path.name} ---")
        
        # Call the cleaning function from before
        clean_company_csv(input_path, COLUMN_TO_CLEAN, output_path)
        
    print("\n--- Batch processing complete. ---")

# --- Run the main function when the script is executed ---
if __name__ == "__main__":
    main()