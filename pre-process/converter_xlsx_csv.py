import pandas as pd
import tkinter as tk
from tkinter import filedialog
import os
from tqdm import tqdm
import glob
import csv


def convert_excel_to_csv(folder_path):
    """
    Finds all .xlsx files in a folder, finds a sheet named 'base' (case-insensitive),
    and converts it to a UTF-8 CSV file, showing progress for each row.

    Args:
        folder_path (str): The path to the folder to process.
    """
    try:
        # Find all .xlsx files in the selected directory
        excel_files = glob.glob(os.path.join(folder_path, '*.xlsx'))

        if not excel_files:
            print(f"No .xlsx files found in the selected folder: {folder_path}")
            return

        print(f"Found {len(excel_files)} Excel file(s) to convert.")

        # Loop through each file without a master progress bar
        for excel_file_path in excel_files:

            file_name = os.path.basename(excel_file_path)

            try:
                # Log the start of processing for the current file
                print(f"\n--- Starting to process: {file_name} ---")

                # --- Logic for case-insensitive sheet finding ---
                # First, get all sheet names from the Excel file
                xls = pd.ExcelFile(excel_file_path)
                sheet_names = xls.sheet_names

                target_sheet_name = None
                # Loop through all sheet names to find a match
                for sheet in sheet_names:
                    # .strip() removes leading/trailing spaces, .lower() makes it lowercase
                    if sheet.strip().lower() == 'base':
                        target_sheet_name = sheet  # Store the original sheet name
                        break  # Stop looking once we've found it

                if target_sheet_name is None:
                    # If no match was found after checking all sheets, raise an error
                    raise ValueError("Sheet 'base' not found (checked case-insensitively with spaces trimmed).")

                # Read the data using the correctly identified sheet name
                print(f"Found sheet '{target_sheet_name}', reading data...")
                df = pd.read_excel(excel_file_path, sheet_name=target_sheet_name)

                # Create the output CSV file path
                base_name_without_ext = os.path.splitext(file_name)[0]
                csv_file_path = os.path.join(folder_path, f"{base_name_without_ext}.csv")

                # --- NEW LOGIC: Write row-by-row with a progress bar ---
                # Open the CSV file for writing
                with open(csv_file_path, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    # Write the header first
                    writer.writerow(df.columns)
                    # Write the data rows with a tqdm progress bar
                    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Writing rows", unit="row"):
                        writer.writerow(row)

                print(f"Successfully converted '{target_sheet_name}' sheet from: {file_name}")

            except ValueError as ve:
                # This error is often raised if the sheet doesn't exist
                print("=" * 50)
                print(f"ERROR: Could not convert file: {file_name}")
                print(f"REASON: {ve}")
                print("=" * 50)
            except Exception as e:
                # If an error occurs with a specific file, print it clearly
                print("=" * 50)
                print(f"ERROR: Could not convert file: {file_name}")
                print(f"REASON: {e}")
                print("=" * 50)

        print("\n--- All conversions complete! ---")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():
    """
    Main function to prompt the user to select a folder and start the conversion.
    """
    # Create a root window and hide it
    root = tk.Tk()
    root.withdraw()

    # Ask the user to select a folder
    print("Opening dialog to select a folder...")
    folder_path = filedialog.askdirectory(
        title="Select the folder containing your Excel files"
    )

    # If the user cancels the dialog, the path will be empty
    if not folder_path:
        print("No folder selected. Exiting.")
        return

    # Run the conversion function
    convert_excel_to_csv(folder_path)


# --- How to use the function ---
# 1. Make sure you have pandas, openpyxl, and tqdm installed:
#    pip install pandas openpyxl tqdm
# 2. Run this script.
# 3. A dialog will appear. Select the folder containing the .xlsx files you want to convert.
# 4. The script will process the 'base' sheet from all Excel files and save them as .csv files.
if __name__ == "__main__":
    main()
