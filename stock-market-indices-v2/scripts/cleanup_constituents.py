import os
import pandas as pd
import re

# --- Configuration ---
# This script should be inside your 'scripts' folder
CONSTITUENTS_DIR = "../data/constituent_lists/"
MASTER_LIST_CSV = r"D:\stock-market-indices-v2\data\master_indices_list.csv"

def cleanup_old_files():
    """
    Identifies and safely deletes old, incorrectly named constituent files.
    """
    print("="*60)
    print("--- Constituent Directory Cleanup Tool ---")
    print("="*60)

    # --- Step 1: Determine what the CORRECT filenames should be ---
    try:
        df_master = pd.read_csv(MASTER_LIST_CSV)
        valid_tickers = df_master['Ticker'].tolist()
    except FileNotFoundError:
        print(f"❌ ERROR: Master list '{MASTER_LIST_CSV}' not found. Cannot determine valid filenames.")
        return

    # Generate the set of all valid, new-style filenames
    valid_filenames = set()
    for ticker in valid_tickers:
        safe_ticker_name = re.sub(r'[\^.:]', '_', ticker)
        valid_filenames.add(f"{safe_ticker_name}.csv")
        
    print(f"Found {len(valid_filenames)} expected filenames based on your master list.")

    # --- Step 2: Scan the directory and find files to delete ---
    if not os.path.isdir(CONSTITUENTS_DIR):
        print(f"Directory '{CONSTITUENTS_DIR}' does not exist. Nothing to clean.")
        return
        
    all_files_in_dir = os.listdir(CONSTITUENTS_DIR)
    files_to_delete = []

    for filename in all_files_in_dir:
        if filename not in valid_filenames:
            files_to_delete.append(filename)

    # --- Step 3: Ask for confirmation before deleting ---
    if not files_to_delete:
        print("\n✅ Directory is already clean! No old files found.")
        return

    print("\n" + "!"*60)
    print("The following old/incorrectly named files were found:")
    print("!"*60)
    for filename in files_to_delete:
        print(f"  - {filename}")
    print("!"*60)

    try:
        confirm = input("\nDo you want to permanently delete these files? (y/n): ")
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        return

    if confirm.lower() == 'y':
        print("\nDeleting files...")
        deleted_count = 0
        for filename in files_to_delete:
            try:
                os.remove(os.path.join(CONSTITUENTS_DIR, filename))
                print(f"  - Deleted {filename}")
                deleted_count += 1
            except OSError as e:
                print(f"  - ❌ Error deleting {filename}: {e}")
        print(f"\n✅ Cleanup complete. Deleted {deleted_count} files.")
    else:
        print("\nCleanup cancelled. No files were deleted.")

if __name__ == "__main__":
    cleanup_old_files()