import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import tkinter as tk
from tkinter import filedialog
import logging
from tqdm import tqdm
import re
import configparser

# --- Configuration ---
# Set up logging to display informational messages in the console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
CONFIG_FILE = 'config.ini'


def read_config():
    """Reads configuration from config.ini."""
    if not os.path.exists(CONFIG_FILE):
        logging.error(f"Configuration file '{CONFIG_FILE}' not found. Please create it.")
        return None

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    try:
        db_config = config['DATABASE']
        settings_config = config['SETTINGS']
        columns_config = config['COLUMNS']

        # Helper to parse comma-separated strings into a list of cleaned-up items
        def parse_cs_string(cs_string):
            return [item.strip().lower() for item in cs_string.split(',') if item.strip()]

        return {
            'host': db_config['host'],
            'user': db_config['user'],
            'password': db_config['password'],
            'database': db_config['database'],
            'target_folder': settings_config['target_folder'],
            'target_sheets': parse_cs_string(settings_config['target_sheets']),
            'exact_match_cols': parse_cs_string(columns_config['exact_match']),
            'starts_with_cols': parse_cs_string(columns_config['starts_with'])
        }
    except KeyError as e:
        logging.error(f"Missing configuration key in {CONFIG_FILE}: {e}")
        return None


def clean_column_name(column_name):
    """Cleans a column name to be used in SQL and for matching."""
    if not isinstance(column_name, str):
        return str(column_name)
    cleaned = column_name.strip().lower()
    cleaned = cleaned.replace(' ', '_').replace('/', '')
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '', cleaned)
    return cleaned


def select_folder():
    """Opens a dialog for the user to select a folder."""
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    folder_path = filedialog.askdirectory(title="Select the Main Folder")
    if not folder_path:
        logging.error("No folder selected. Exiting.")
        exit()
    logging.info(f"Folder selected: {folder_path}")
    return folder_path


def create_db_connection(host_name, user_name, user_password, db_name):
    """
    Creates a database connection.
    If the database does not exist, it will be created.
    """
    connection = None
    try:
        # First, connect to the MySQL server without specifying the database
        server_connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        cursor = server_connection.cursor()

        # Check if the database exists
        cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
        result = cursor.fetchone()

        if result:
            logging.info(f"Database '{db_name}' already exists.")
        else:
            logging.info(f"Database '{db_name}' not found. Creating it...")
            cursor.execute(f"CREATE DATABASE `{db_name}`")
            logging.info(f"Database '{db_name}' created successfully.")

        cursor.close()
        server_connection.close()

        # Now, connect to the specific database
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        logging.info(f"MySQL connection to database '{db_name}' successful")

    except Error as e:
        logging.error(f"Error '{e}' occurred during database connection/creation.")
        return None

    return connection


def create_table_from_dataframe(connection, table_name, df):
    """Creates a MySQL table from a DataFrame if it does not already exist."""
    cursor = connection.cursor()

    # Sanitize table name to be valid in SQL
    sanitized_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

    # Define columns based on DataFrame for the CREATE statement
    columns_sql = []
    for col in df.columns:
        # Basic type inference; can be improved if needed
        dtype = df[col].dtype
        sql_type = "VARCHAR(255)"  # Default
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "INT"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = "DATETIME"

        columns_sql.append(f"`{col}` {sql_type}")

    # Use CREATE TABLE IF NOT EXISTS to avoid dropping data and to create only when needed
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{sanitized_table_name}` ({', '.join(columns_sql)})"

    try:
        logging.info(f"Ensuring table '{sanitized_table_name}' exists...")
        cursor.execute(create_table_query)
        connection.commit()
        logging.info(f"Table '{sanitized_table_name}' is ready. New data will be appended.")
        return True
    except Error as e:
        logging.error(f"Error creating table '{sanitized_table_name}': {e}")
        return False
    finally:
        cursor.close()


def process_excel_files(folder_path, db_connection, config):
    """Finds and processes excel files in the target subfolder."""
    target_folder = config['target_folder']
    target_sheets = config['target_sheets']
    exact_match_cols = config['exact_match_cols']
    starts_with_cols = config['starts_with_cols']

    abd_folder_path = None
    for item in os.listdir(folder_path):
        if item.lower() == target_folder.lower():
            abd_folder_path = os.path.join(folder_path, item)
            break

    if not abd_folder_path:
        logging.error(f"Could not find a folder named '{target_folder}' in the selected directory.")
        return

    logging.info(f"Found '{target_folder}' folder at: {abd_folder_path}")

    for filename in os.listdir(abd_folder_path):
        # UPDATED: Look for both .xlsx and .xlsb files.
        if filename.lower().endswith(('.xlsx', '.xlsb')):
            file_path = os.path.join(abd_folder_path, filename)
            logging.info(f"\n--- Processing file: {filename} ---")

            try:
                # Determine the correct engine for pandas to use based on the file extension.
                engine = 'openpyxl' if filename.lower().endswith('.xlsx') else 'pyxlsb'

                xls = pd.ExcelFile(file_path, engine=engine)
                target_sheet = None
                for sheet_name in xls.sheet_names:
                    if sheet_name.lower() in target_sheets:
                        target_sheet = sheet_name
                        break

                if not target_sheet:
                    logging.warning(f"No target sheet ({', '.join(target_sheets)}) found in {filename}. Skipping.")
                    continue

                logging.info(f"Reading sheet '{target_sheet}' from {filename}.")
                df = pd.read_excel(xls, sheet_name=target_sheet)

                # --- NEW: Column Filtering Logic ---
                original_columns = df.columns.tolist()
                col_map = {clean_column_name(c): c for c in original_columns}
                cleaned_actual_cols = list(col_map.keys())

                columns_to_keep_original_names = []
                found_cols = set()  # To avoid adding the same column twice

                # 1. Find exact matches
                for target_col in exact_match_cols:
                    # The target_col from config is already cleaned
                    if target_col in cleaned_actual_cols and target_col not in found_cols:
                        columns_to_keep_original_names.append(col_map[target_col])
                        found_cols.add(target_col)

                # 2. Find "starts with" matches
                for target_prefix in starts_with_cols:
                    for cleaned_col in cleaned_actual_cols:
                        if cleaned_col.startswith(target_prefix) and cleaned_col not in found_cols:
                            columns_to_keep_original_names.append(col_map[cleaned_col])
                            found_cols.add(cleaned_col)
                            break  # Move to next prefix once one is found

                if not columns_to_keep_original_names:
                    logging.warning(f"Could not find any of the specified columns in {filename}. Skipping.")
                    continue

                logging.info(f"Found {len(columns_to_keep_original_names)} matching columns.")

                # Create a new DataFrame with only the desired columns
                filtered_df = df[columns_to_keep_original_names]
                # Clean the column names of the final DataFrame for database insertion
                filtered_df.columns = [clean_column_name(col) for col in filtered_df.columns]
                # --- END: Column Filtering Logic ---

                # Create table based on the filtered DataFrame
                table_name = os.path.splitext(filename)[0]
                if not create_table_from_dataframe(db_connection, table_name, filtered_df):
                    continue  # Skip to next file if table creation fails

                # Insert data
                cursor = db_connection.cursor()
                cols = ", ".join([f"`{c}`" for c in filtered_df.columns])
                placeholders = ", ".join(["%s"] * len(filtered_df.columns))
                insert_sql = f"INSERT INTO `{re.sub(r'[^a-zA-Z0-9_]', '_', table_name)}` ({cols}) VALUES ({placeholders})"

                logging.info(f"Inserting data into table '{table_name}'...")

                # Convert DataFrame to list of tuples for insertion
                data_to_insert = [tuple(row) for row in
                                  filtered_df.where(pd.notnull(filtered_df), None).itertuples(index=False)]

                with tqdm(total=len(data_to_insert), desc=f"Uploading {filename}") as pbar:
                    for record in data_to_insert:
                        try:
                            cursor.execute(insert_sql, record)
                            pbar.update(1)
                        except Error as e:
                            logging.error(f"Error inserting row: {record}. Error: {e}")

                db_connection.commit()
                cursor.close()
                logging.info(f"Successfully inserted {len(data_to_insert)} rows into '{table_name}'.")

            except Exception as e:
                logging.error(f"An error occurred while processing {filename}: {e}")


def main():
    """Main function to run the utility."""
    config = read_config()
    if not config:
        return

    folder = select_folder()

    connection = create_db_connection(
        config['host'], config['user'], config['password'], config['database']
    )

    if connection:
        process_excel_files(folder, connection, config)
        connection.close()
        logging.info("\nProcess finished. MySQL connection is closed.")


if __name__ == "__main__":
    main()

