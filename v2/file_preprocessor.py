import pandas as pd
from datetime import datetime
import tkinter as tk
from tkinter import filedialog
import os


def reshape_payroll_data(file_path, fiscal_year_start):
    """
    Reshapes payroll data from a wide format to a long format.

    Args:
        file_path (str): The path to the input XLSX file.
        fiscal_year_start (int): The starting year of the fiscal period (e.g., 2024).

    Returns:
        pandas.DataFrame: A DataFrame with the reshaped data,
                          or None if an error occurs.
    """
    try:
        # Read the Excel file, specifying that the first two rows are the header.
        df = pd.read_excel(file_path, header=[0, 1])

        # Identify and filter out the summary columns at the end of the sheet.
        # We define the valid months we expect to see.
        valid_months = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar']

        # Get the first level of the column index (which contains month names and 'Unnamed' placeholders)
        top_level_cols = df.columns.get_level_values(0)

        # Create a boolean mask to select only the columns we want to keep.
        # We keep the first column (Row Labels) and any column whose top-level name is a valid month.
        cols_to_keep_mask = [True] + [col.strip() in valid_months for col in top_level_cols[1:]]

        # Apply the mask to the DataFrame
        df = df.iloc[:, cols_to_keep_mask]

        # The first level of the column MultiIndex contains the months, but with
        # many 'Unnamed' columns. We need to forward-fill the correct month names.
        new_cols = []
        last_month = ''
        for month, metric in df.columns:
            # If the month column is not 'Unnamed', update our last_month variable
            if 'Unnamed' not in str(month):
                last_month = month.strip()

            # The first column is special ('Unnamed: 0_level_0', 'Row Labels')
            if 'Row Labels' in metric:
                new_cols.append(('EMPLID', ''))
            else:
                new_cols.append((last_month, metric))

        # Assign the cleaned MultiIndex back to the DataFrame
        df.columns = pd.MultiIndex.from_tuples(new_cols)

        # Rename the first column from a MultiIndex tuple to a simple string 'EMPLID'
        df.rename(columns={'EMPLID': 'EMPLID'}, inplace=True)
        df.set_index('EMPLID', inplace=True)

        # Stack the DataFrame. This pivots the months into a new index level.
        # Added future_stack=True to silence the warning.
        df_stacked = df.stack(level=0, future_stack=True)

        # Reset the index to turn the stacked levels (EMPLID, Month) back into columns
        df_stacked.reset_index(inplace=True)

        # Rename the columns as requested by the user
        df_stacked.rename(columns={
            'level_1': 'Month',
            'Sum of Total pay': 'gross pay',
            'Sum of ER NIC': 'Sum of ER NIC'
        }, inplace=True)

        # Clean up the data: remove any rows that are totals (non-numeric EMPLID)
        df_stacked = df_stacked[pd.to_numeric(df_stacked['EMPLID'], errors='coerce').notna()]
        df_stacked['EMPLID'] = df_stacked['EMPLID'].astype(int)

        # Define the fiscal year to correctly map months to dates
        month_to_year = {
            'Apr': fiscal_year_start, 'May': fiscal_year_start, 'Jun': fiscal_year_start,
            'Jul': fiscal_year_start, 'Aug': fiscal_year_start, 'Sep': fiscal_year_start,
            'Oct': fiscal_year_start, 'Nov': fiscal_year_start, 'Dec': fiscal_year_start,
            'Jan': fiscal_year_start + 1, 'Feb': fiscal_year_start + 1, 'Mar': fiscal_year_start + 1
        }

        def convert_month_to_date(month_str):
            year = month_to_year.get(month_str.strip(), 9999)  # Default to a bad year if not found
            date_str = f"{month_str.strip()} {year}"
            return datetime.strptime(date_str, "%b %Y")

        df_stacked['Month'] = df_stacked['Month'].apply(convert_month_to_date)

        # Format the 'Month' column to show only the date, not the time
        df_stacked['Month'] = pd.to_datetime(df_stacked['Month']).dt.date

        # Sort the data by Month and then by Employee ID
        df_sorted = df_stacked.sort_values(by=['Month', 'EMPLID'])

        # Select, reorder the final columns, and drop rows where there is no pay data
        final_df = df_sorted[['EMPLID', 'Month', 'gross pay', 'Sum of ER NIC']].dropna(subset=['gross pay'])

        return final_df

    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def main():
    """
    Main function to run the data reshaping process by selecting a folder.
    """
    # Create a root window and hide it
    root = tk.Tk()
    root.withdraw()

    # Ask the user to select a folder
    print("Opening dialog to select a folder...")
    folder_path = filedialog.askdirectory(
        title="Select the folder containing 'raw_salary.xlsx'"
    )

    # If the user cancels the dialog, the path will be empty
    if not folder_path:
        print("No folder selected. Exiting.")
        return

    # --- NEW CHANGE ---
    # Extract the folder name to use as the fiscal year
    try:
        # os.path.basename gets the last part of the path (the folder name)
        folder_name = os.path.basename(folder_path)
        fiscal_year = int(folder_name)
        print(f"Detected fiscal year: {fiscal_year}")
    except (ValueError, TypeError):
        print(f"Error: The selected folder name '{folder_name}' is not a valid year.")
        print("Please select a folder named with a four-digit year (e.g., '2024').")
        return
    # --- END CHANGE ---

    # Define the input and output filenames
    input_filename = "raw_salary.xlsx"
    output_filename = "salary.xlsx"

    # Create the full paths for the input and output files
    input_file_path = os.path.join(folder_path, input_filename)
    output_file_path = os.path.join(folder_path, output_filename)

    # Check if the input file exists in the selected folder
    if not os.path.exists(input_file_path):
        print(f"Error: '{input_filename}' not found in the selected folder.")
        print(f"Looked in: {folder_path}")
        return

    # Run the main reshaping function, passing the detected fiscal year
    print(f"Processing {input_file_path}...")
    reshaped_data = reshape_payroll_data(file_path=input_file_path, fiscal_year_start=fiscal_year)

    # If the data was processed successfully, save it to the new file
    if reshaped_data is not None:
        # Save to an Excel file
        reshaped_data.to_excel(output_file_path, index=False)
        print(f"Data successfully reshaped and saved to {output_file_path}")


# --- How to use the function ---
# 1. Make sure you have pandas and openpyxl installed:
#    pip install pandas openpyxl
# 2. Run this script. A dialog will appear to select a folder named with the fiscal year (e.g., "2024").
# 3. The script will look for 'raw_salary.xlsx' and save 'salary.xlsx' in that folder.
if __name__ == "__main__":
    main()
