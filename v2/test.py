import pandas as pd
import tkinter as tk
from tkinter import filedialog
from datetime import datetime


def process_salary_data(input_file_path, output_file_path):
    """
    Reads salary data from an Excel file, processes it, and saves the
    consolidated data to a new separate Excel file.

    Args:
        input_file_path (str): The path to the input Excel file.
        output_file_path (str): The path to save the new output Excel file.
    """
    try:
        # Read the Excel file, assuming data is on the first sheet.
        # The header is on the second row (index 1).
        df = pd.read_excel(input_file_path, header=1, sheet_name=0)

        # The first column is 'Row Labels' which contains employee IDs.
        # Let's clean up the column names, as they might have extra spaces.
        df.columns = df.columns.str.strip()

        # Define the months and their corresponding start dates for the fiscal year
        fiscal_year_map = {
            "Apr": datetime(2024, 4, 1), "May": datetime(2024, 5, 1),
            "Jun": datetime(2024, 6, 1), "Jul": datetime(2024, 7, 1),
            "Aug": datetime(2024, 8, 1), "Sep": datetime(2024, 9, 1),
            "Oct": datetime(2024, 10, 1), "Nov": datetime(2024, 11, 1),
            "Dec": datetime(2024, 12, 1), "Jan": datetime(2025, 1, 1),
            "Feb": datetime(2025, 2, 1), "Mar": datetime(2025, 3, 1)
        }
        months = list(fiscal_year_map.keys())

        # The columns for "Sum of Total pay" and "Sum of ER NIC" repeat every 5 columns
        total_pay_indices = [1 + i * 5 for i in range(12)]
        er_nic_indices = [5 + i * 5 for i in range(12)]

        # --- Consolidate all monthly data ---
        all_months_data = []

        for i, month_name in enumerate(months):
            pay_col_index = total_pay_indices[i]
            nic_col_index = er_nic_indices[i]

            # Check if columns exist before accessing them
            if pay_col_index < len(df.columns) and nic_col_index < len(df.columns):
                # Extract data for the current month
                month_df = df.iloc[:, [0, pay_col_index, nic_col_index]].copy()
                # Rename columns to the new requested names
                month_df.columns = ['EMPLID', 'Gross Pay', 'ER_NIC_SUM']

                # Add the month column with the date value
                month_df['Month'] = fiscal_year_map[month_name]

                all_months_data.append(month_df)
            else:
                print(f"Warning: Columns for {month_name} not found. Skipping.")

        # Combine all monthly dataframes into a single one
        consolidated_df = pd.concat(all_months_data, ignore_index=True)

        # Clean up the final dataframe
        consolidated_df.dropna(subset=['EMPLID'], inplace=True)
        consolidated_df = consolidated_df[consolidated_df['EMPLID'] != 'Grand Total']

        # Reorder columns to the desired format
        consolidated_df = consolidated_df[['EMPLID', 'Month', 'Gross Pay', 'ER_NIC_SUM']]

        # Save the consolidated dataframe to a new Excel file.
        consolidated_df.to_excel(output_file_path, sheet_name='Total', index=False)
        print(f"Successfully created consolidated file at: {output_file_path}")

    except FileNotFoundError:
        print(f"Error: The file '{input_file_path}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == '__main__':
    # Set up the tkinter root window
    root = tk.Tk()
    root.withdraw()  # We don't need a full GUI, so keep the root window from appearing

    # Open a file dialog to select the input Excel file
    input_path = filedialog.askopenfilename(
        title="Select the salary Excel file",
        filetypes=[("Excel Files", "*.xlsx"), ("All files", "*.*")]
    )

    # If the user selects a file, proceed to ask for a save location
    if input_path:
        output_path = filedialog.asksaveasfilename(
            title="Save the new consolidated file as...",
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")]
        )

        # If the user also chooses a save location, run the processing function
        if output_path:
            process_salary_data(input_path, output_path)
        else:
            print("No output location selected. Operation cancelled.")
    else:
        print("No input file selected. Operation cancelled.")

