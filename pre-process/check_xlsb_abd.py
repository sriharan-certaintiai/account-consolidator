import os
import re
import pandas as pd
import tkinter as tk
from tkinter import filedialog
from tqdm import tqdm


def find_sheet_name(xls_file):
    """
    Checks for a sheet named 'data' or 'base' (case-insensitive) in an Excel file.
    Returns the actual sheet name if found, otherwise None.
    """
    sheet_names = xls_file.sheet_names
    for name in sheet_names:
        if name.lower() in ['data', 'base', 'sheet1']:
            return name
    return None


def validate_columns(df, exact_cols, prefix_cols, file_path, log_func=print):
    """
    Validates if the required columns exist in the DataFrame.
    Reports missing and found columns to the console using a logger function.
    """
    all_found = True
    df_columns = df.columns.to_list()
    filename = os.path.basename(file_path)

    log_func(f"[INFO]    Validating columns for '{filename}'...")

    # --- Check for columns that must match exactly ---
    for col in exact_cols:
        if col in df_columns:
            log_func(f"[FOUND]   File: '{filename}' - Found exact column: '{col}'")
        else:
            log_func(f"[MISSING] File: '{filename}' - Exact column not found: '{col}'")
            all_found = False

    # --- Check for columns that must match a prefix ---
    for prefix in prefix_cols:
        # Find the column that matches the prefix
        matching_col = next((df_col for df_col in df_columns if str(df_col).startswith(prefix)), None)
        if matching_col:
            log_func(f"[FOUND]   File: '{filename}' - Found column with prefix '{prefix}': '{matching_col}'")
        else:
            log_func(f"[MISSING] File: '{filename}' - No column starts with: '{prefix}'")
            all_found = False

    return all_found


def process_folder(folder_path):
    """
    Main function to iterate through files in a folder and validate them.
    """
    if not folder_path:
        print("No folder selected. Exiting.")
        return

    print(f"--- Starting validation in folder: {folder_path} ---\n")

    # Define the required columns
    exact_match_columns = [
        'EMPLID', 'JOB_CODE_DESCRIPTION', 'BAND',
        'CURRENT_LOCATION_DESCRIPTION', 'PROJECT_ID', 'PROJECT_DESCRIPTION',
        'PROJECT_TYPE_DESC', 'CUSTOMER_NAME', 'PROGRAM_MANAGER_NAME'
    ]

    prefix_match_columns = [
        'Technical/', 'PROJECT_PRICING_TYPE'
    ]

    # Regex to match filenames like 'MM-YYYY.xlsx' or 'MM-YYYY.xlsb'
    file_pattern = re.compile(r'^\d{2}-\d{4}\.(xlsx|xlsb)$', re.IGNORECASE)

    # Filter for files that match the pattern to use with the progress bar
    matching_files = [f for f in os.listdir(folder_path) if file_pattern.match(f)]
    found_files_count = len(matching_files)
    validated_files_count = 0

    if found_files_count > 0:
        # Use tqdm.write for logging to prevent interfering with the progress bar
        log = tqdm.write
        for filename in tqdm(matching_files, desc="Validating files", unit="file"):
            file_path = os.path.join(folder_path, filename)
            log(f"\n--- Processing file: {filename} ---")

            try:
                # Determine the correct engine based on file extension
                engine = 'pyxlsb' if filename.lower().endswith('.xlsb') else 'openpyxl'

                # Load the excel file to check for sheets
                xls = pd.ExcelFile(file_path, engine=engine)
                sheet_to_read = find_sheet_name(xls)

                if sheet_to_read:
                    log(f"[INFO]    Found sheet: '{sheet_to_read}'")
                    log(f"[INFO]    Loading data from sheet... (This may take a moment for large files)")
                    # If a valid sheet is found, read it into a dataframe
                    df = pd.read_excel(xls, sheet_name=sheet_to_read)

                    # Validate the columns in the dataframe, passing the log function
                    if validate_columns(df, exact_match_columns, prefix_match_columns, file_path, log_func=log):
                        validated_files_count += 1
                        log(f"[SUCCESS] '{filename}' passed all checks.")
                    else:
                        log(f"[FAILURE] '{filename}' has validation issues.")
                else:
                    log(f"[ERROR]   File: '{filename}' - Neither 'data' nor 'base' sheet was found.")

            except Exception as e:
                log(f"[ERROR]   Could not read file '{filename}'. Reason: {e}")

    print("\n--- Validation Complete ---")
    if found_files_count == 0:
        print("No files matching the 'MM-YYYY.xlsx' pattern were found in the selected folder.")
    else:
        print(f"Total files matching pattern 'MM-YYYY.xlsx': {found_files_count}")
        print(f"Files that passed all checks: {validated_files_count}")
        if validated_files_count < found_files_count:
            print("Issues were found in one or more files. Please see the messages above.")
        else:
            print("All matching files passed validation successfully!")
    print("--------------------------")


def select_folder_and_run():
    """
    Opens a dialog to select a folder and then runs the validation process.
    """
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    folder_path = filedialog.askdirectory(title="Select Folder Containing Excel Files")
    process_folder(folder_path)


if __name__ == "__main__":
    select_folder_and_run()


