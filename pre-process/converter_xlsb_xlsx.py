import os
import win32com.client as win32
import tkinter as tk
from tkinter import filedialog


def batch_convert_with_gui():
    """
    Opens a GUI to select a root folder, then recursively finds and converts
    all .xlsb files to .xlsx within that folder and its subdirectories.
    """
    # --- Step 1: Use Tkinter to select the root folder ---
    root = tk.Tk()
    root.withdraw()  # Hide the small tkinter window

    print("Please select the root folder containing your .xlsb files...")
    folder_path = filedialog.askdirectory(title="Select the Root Folder to Scan")

    if not folder_path:
        print("No folder selected. Exiting.")
        return

    print(f"Scanning folder: {folder_path}\n")

    excel = None
    try:
        # --- Step 2: Start one instance of Excel for all files (much faster) ---
        excel = win32.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False

        # --- Step 3: Walk through the directory tree ---
        for dirpath, _, filenames in os.walk(folder_path):
            for filename in filenames:
                if filename.lower().endswith(".xlsb"):
                    xlsb_path = os.path.join(dirpath, filename)
                    xlsx_path = os.path.splitext(xlsb_path)[0] + '.xlsx'

                    try:
                        print(f"Found & converting: {xlsb_path}")

                        # Open the workbook using its absolute, cleaned path
                        # ▼▼▼ THIS LINE IS THE FIX ▼▼▼
                        wb = excel.Workbooks.Open(os.path.abspath(xlsb_path))

                        # Save in the .xlsx format, also using an absolute path for safety
                        # ▼▼▼ THIS LINE IS ALSO UPDATED FOR BEST PRACTICE ▼▼▼
                        wb.SaveAs(os.path.abspath(xlsx_path), FileFormat=51)

                        wb.Close()
                        print(f"  -> Successfully saved as {xlsx_path}")

                    except Exception as e:
                        print(f"  -> FAILED to convert {filename}. Error: {e}")

    except Exception as e:
        print(f"A critical error occurred: {e}")
    finally:
        # --- Step 4: Close the Excel application ---
        if excel:
            excel.Quit()
        print("\nProcessing complete.")


# --- Run the main function ---
if __name__ == "__main__":
    batch_convert_with_gui()