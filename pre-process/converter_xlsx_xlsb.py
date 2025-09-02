import os
import sys
import win32com.client as win32
from tkinter import Tk, filedialog
from tqdm import tqdm

"""
A Python script to batch convert .xlsx (Open XML Spreadsheet) files in a
user-selected folder to .xlsb (Binary Spreadsheet) format.

This script leverages the pywin32 library to interact with the Microsoft Excel
application via its Component Object Model (COM) interface. A single instance
of Excel is launched in the background to efficiently process all files.

It features detailed console logging and a progress bar for monitoring.

Prerequisites:
1.  Operating System: Windows
2.  Software: Microsoft Excel must be installed.
3.  Python Libraries: pywin32 (`pip install pywin32`), tqdm (`pip install tqdm`)

Usage:
    Run the script directly from the command line without arguments:
    python xlsx_to_xlsb_converter.py

    A dialog box will appear, prompting you to select a folder.
"""


def select_folder():
    """
    Opens a GUI dialog for the user to select a folder.

    Returns:
        str: The absolute path of the selected folder, or an empty string if cancelled.
    """
    root = Tk()
    root.withdraw()  # Hide the main tkinter window
    folder_selected = filedialog.askdirectory(
        initialdir=os.getcwd(),
        title="Please select the folder containing your .xlsx files"
    )
    return folder_selected


def convert_all_xlsx_in_folder(folder_path):
    """
    Finds all .xlsx files in a given folder and converts them to .xlsb.
    One instance of Excel is used for all conversions for efficiency.

    Args:
        folder_path (str): The full path to the folder containing .xlsx files.
    """
    excel_app = None
    try:
        print("[LOG] Starting Microsoft Excel application in the background...")
        # Start a single instance of the Excel application in the background.
        excel_app = win32.Dispatch('Excel.Application')
        excel_app.Visible = False
        excel_app.DisplayAlerts = False
        print("[LOG] Excel application started successfully.")

        # Find all files in the directory with the .xlsx extension.
        print("[LOG] Searching for .xlsx files...")
        xlsx_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.xlsx')]

        if not xlsx_files:
            print("\n[INFO] No .xlsx files were found in the selected folder.")
            return

        print(f"[LOG] Found {len(xlsx_files)} .xlsx file(s). Starting conversion process.")
        successful_conversions = 0

        # Loop through each found file and convert it, using tqdm for a progress bar.
        for filename in tqdm(xlsx_files, desc="Converting Files", unit="file"):
            input_path = os.path.join(folder_path, filename)
            output_path = os.path.splitext(input_path)[0] + '.xlsb'

            # Use absolute paths to be safe.
            input_path_abs = os.path.abspath(input_path)
            output_path_abs = os.path.abspath(output_path)
            workbook = None  # Initialize workbook to None

            try:
                tqdm.write(f"\n[PROCESS] Converting '{filename}'...")
                tqdm.write(f"  [STEP] Opening workbook...")
                # Open the source workbook.
                workbook = excel_app.Workbooks.Open(input_path_abs)
                tqdm.write(f"  [STEP] Workbook loaded.")

                # The FileFormat constant for .xlsb is 50 (xlExcelBinaryWorkbook).
                file_format_xlsb = 50

                tqdm.write(f"  [STEP] Saving as .xlsb format...")
                # Save the workbook in the new format.
                workbook.SaveAs(output_path_abs, FileFormat=file_format_xlsb)

                tqdm.write(f"  [SUCCESS] Saved as '{os.path.basename(output_path)}'")
                successful_conversions += 1
            except Exception as e:
                tqdm.write(f"  [ERROR] FAILED to convert '{filename}'.")
                tqdm.write(f"    -> Details: {e}")
            finally:
                # Close the workbook if it was opened.
                if workbook:
                    tqdm.write(f"  [STEP] Closing workbook...")
                    workbook.Close(SaveChanges=False)
                    tqdm.write(f"  [STEP] Workbook closed.")

        print(
            f"\n[COMPLETE] Batch conversion finished. {successful_conversions}/{len(xlsx_files)} files converted successfully.")

    except Exception as e:
        print(f"\n[FATAL] An unexpected error occurred during the process: {e}")
    finally:
        # Crucially, always ensure the Excel application process is terminated.
        if excel_app:
            print("[LOG] Closing Microsoft Excel application...")
            excel_app.Quit()
            # Release the COM object.
            del excel_app
            print("[LOG] Excel application closed.")


if __name__ == '__main__':
    print("This script will convert all .xlsx files in a selected folder to .xlsb format.")

    # Prompt user to select a folder.
    target_folder = select_folder()

    if not target_folder:
        print("\nNo folder was selected. Exiting.")
        sys.exit(0)

    print(f"\nSelected folder: {target_folder}")

    # Execute the conversion process on the selected folder.
    convert_all_xlsx_in_folder(target_folder)

