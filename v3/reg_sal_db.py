import os
import pandas as pd
from sqlalchemy import create_engine, text
import re
import sys

# --- Database Configuration ---
# --- You can change these values if your MySQL setup is different ---
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'


# -----------------------------

def process_data_folder(root_folder):
    """
    Main function to process the folder structure, create a database,
    and import data from Excel files into MySQL tables.
    """
    if not os.path.isdir(root_folder):
        print(f"Error: The provided path '{root_folder}' is not a valid directory.")
        sys.exit(1)

    # 1. Determine database name from the root folder's name.
    db_name = os.path.basename(os.path.normpath(root_folder))
    # Sanitize database name to be a valid MySQL identifier
    db_name = re.sub(r'[^a-zA-Z0-9_]', '_', db_name)
    print(f"Database name will be: `{db_name}`")

    # 2. Connect to MySQL server and create the database if it doesn't exist.
    try:
        # Engine to connect to the MySQL server instance (no specific DB)
        server_engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}')
        with server_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}`"))
            print(f"Database `{db_name}` is ready.")

        # Engine to connect directly to the newly created/verified database
        db_engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{db_name}')

    except Exception as e:
        print(f"\n--- DATABASE CONNECTION ERROR ---")
        print(f"Could not connect to MySQL server at '{DB_HOST}'.")
        print("Please ensure MySQL is running and the credentials (user, password) are correct.")
        print(f"Error details: {e}")
        sys.exit(1)

    # 3. Iterate through subdirectories to find year folders (e.g., '2023', '2024').
    for item in os.listdir(root_folder):
        item_path = os.path.join(root_folder, item)

        # Check if the item is a directory and its name is a 4-digit year.
        if os.path.isdir(item_path) and re.match(r'^\d{4}$', item):
            year = item
            print(f"\n--- Processing Year: {year} ---")

            # --- Process Regional.xlsx ---
            regional_file_path = os.path.join(item_path, 'Regional.xlsx')
            if os.path.exists(regional_file_path):
                try:
                    df_regional = pd.read_excel(regional_file_path)
                    # Add the 'year' column from the folder name.
                    df_regional['year'] = year
                    print(f"Read {len(df_regional)} rows from Regional.xlsx")

                    # Sanitize column names for SQL compatibility (lowercase, underscores).
                    df_regional.columns = [str(c).strip().lower().replace(' ', '_').replace('.', '') for c in
                                           df_regional.columns]

                    # Write DataFrame to the 'regional' table. Appends data if table exists.
                    df_regional.to_sql('regional', db_engine, if_exists='append', index=False)
                    print("-> Successfully loaded data into 'regional' table.")
                except Exception as e:
                    print(f"-> Error processing {regional_file_path}: {e}")
            else:
                print("-> Warning: 'Regional.xlsx' not found.")

            # --- Process Salary.xlsx ---
            salary_file_path = os.path.join(item_path, 'Salary.xlsx')
            if os.path.exists(salary_file_path):
                try:
                    df_salary = pd.read_excel(salary_file_path)
                    # Add the 'year' column.
                    df_salary['year'] = year
                    print(f"Read {len(df_salary)} rows from Salary.xlsx")

                    # Handle the potentially missing 'ER_NIC_SUM' column for consistency.
                    if 'ER_NIC_SUM' not in df_salary.columns:
                        # Use .strip() on existing columns to avoid key errors with trailing spaces
                        if 'ER_NIC_SUM ' not in [str(c).strip() for c in df_salary.columns]:
                            df_salary['ER_NIC_SUM'] = None  # Add column with null values
                            print("   (Note: 'ER_NIC_SUM' column was not found and has been added as NULL)")

                    # Sanitize column names for SQL.
                    df_salary.columns = [str(c).strip().lower().replace(' ', '_').replace('(', '').replace(')', '') for
                                         c in df_salary.columns]

                    # Write DataFrame to the 'salary' table.
                    df_salary.to_sql('salary', db_engine, if_exists='append', index=False)
                    print("-> Successfully loaded data into 'salary' table.")
                except Exception as e:
                    print(f"-> Error processing {salary_file_path}: {e}")
            else:
                print("-> Warning: 'Salary.xlsx' not found.")


if __name__ == "__main__":
    folder_path = input("Please enter the full path to your root data folder: ")
    process_data_folder(folder_path)
    print("\n✅ Processing complete.")
