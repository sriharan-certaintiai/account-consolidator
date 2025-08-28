import pandas as pd
import tkinter as tk
from tkinter import filedialog
import os
import glob


def check_columns_in_excel(folder_path, required_columns_map):
    """
    Checks all .xlsx files in a folder for a 'base' or 'data' sheet (or falls back
    to the first sheet) and verifies if it contains a list of required columns.

    Args:
        folder_path (str): The path to the folder to process.
        required_columns_map (dict): A dictionary of required column names.
    """
    try:
        # Find all .xlsx files in the selected directory
        excel_files = glob.glob(os.path.join(folder_path, '*.xlsx'))

        if not excel_files:
            print(f"No .xlsx files found in the selected folder: {folder_path}")
            return

        print(f"\nFound {len(excel_files)} Excel file(s) to check.")
        print("-" * 50)

        required_columns = list(required_columns_map.keys())

        # Loop through each file
        for excel_file_path in excel_files:
            file_name = os.path.basename(excel_file_path)
            print(f"Checking file: {file_name}")

            try:
                # --- MODIFIED LOGIC: Find 'base', 'data', or use first sheet ---
                xls = pd.ExcelFile(excel_file_path)
                sheet_names = xls.sheet_names

                target_sheet_name = None

                # First, look for 'base'
                for sheet in sheet_names:
                    if sheet.strip().lower() == 'base':
                        target_sheet_name = sheet
                        break

                # If 'base' isn't found, look for 'data'
                if target_sheet_name is None:
                    for sheet in sheet_names:
                        if sheet.strip().lower() == 'data':
                            target_sheet_name = sheet
                            break

                # If neither is found, use the first sheet as a fallback
                if target_sheet_name is None:
                    if sheet_names:
                        target_sheet_name = sheet_names[0]  # Get the first sheet
                        print(f"  - WARNING: Neither 'base' nor 'data' found. Using first sheet: '{target_sheet_name}'")
                    else:
                        print("  - Status: ❌ This Excel file contains no sheets.")
                        print("-" * 50)
                        continue  # Skip to the next file

                if target_sheet_name is None:
                    # This case should now only be hit if the workbook is empty
                    print("  - Status: ❌ Could not find any sheets to process.")
                    print("-" * 50)
                    continue
                # --- END of modified logic ---

                # Read only the header of the sheet to be efficient
                df = pd.read_excel(excel_file_path, sheet_name=target_sheet_name, nrows=0)

                # Standardize the actual column names from the file
                actual_columns = [str(col).strip() for col in df.columns]
                actual_columns_std = [col.lower() for col in actual_columns]

                # --- Flexible column matching logic ---
                missing_columns = []
                for req_col in required_columns:
                    found = False
                    req_col_std = req_col.lower()

                    # Special check for the 'TECHNICAL/BSG/SUPPORT' column
                    if req_col_std == 'technical/bsg/support':
                        for act_col_std in actual_columns_std:
                            if act_col_std.startswith('technical/'):
                                found = True
                                break
                    else:  # Standard exact match for all other columns
                        if req_col_std in actual_columns_std:
                            found = True

                    if not found:
                        missing_columns.append(req_col)

                if not missing_columns:
                    print(f"  - Status: ✅ All required columns are present in sheet '{target_sheet_name}'.")
                else:
                    print(f"  - Status: ⚠️ The following columns are MISSING from sheet '{target_sheet_name}':")
                    for col in sorted(missing_columns):
                        print(f"    - {col}")

            except Exception as e:
                print(f"  - Status: ❌ An error occurred while processing this file.")
                print(f"  - Error details: {e}")

            finally:
                # Print a separator for clarity between files
                print("-" * 50)

        print("Column check complete.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    """
    Main function to get user input and start the checking process.
    """
    # Define the column mapping directly in the script
    ABD_COLUMN_MAP = {
        'EMPLID': 'EMPLID',
        'TECHNICAL/BSG/SUPPORT': 'Function',
        'JOB_CODE_DESCRIPTION': 'Designation',
        'BAND': 'BAND',
        'PROGRAM_MANAGER_NAME': 'PROGRAM_MANAGER_NAME'
    }

    print("Will check for the following columns:")
    for col in ABD_COLUMN_MAP.keys():
        if col == 'TECHNICAL/BSG/SUPPORT':
            print(f"- Any column starting with 'technical/' (for '{col}')")
        else:
            print(f"- {col}")
    print("-" * 50)

    # Create a root window and hide it
    root = tk.Tk()
    root.withdraw()

    # Ask the user to select a folder
    print("Opening dialog to select a folder...")
    folder_path = filedialog.askdirectory(
        title="Select the folder containing your Excel files"
    )

    if not folder_path:
        print("No folder selected. Exiting.")
        return

    # Run the main checking function
    check_columns_in_excel(folder_path, ABD_COLUMN_MAP)


# --- How to use the script ---
# 1. Make sure you have pandas and openpyxl installed:
#    pip install pandas openpyxl
# 2. Run this script.
# 3. A dialog will appear. Select the folder containing your .xlsx files.
# 4. The script will automatically check for the columns defined in ABD_COLUMN_MAP.
if __name__ == "__main__":
    main()
