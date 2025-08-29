# main.py

import os
import re
from tkinter import Tk, filedialog
import config
import db_operations
import validator
import exporter
from file_preprocessor import preprocess_regional_file


def main():
    """Main function to drive the data processing pipeline."""
    Tk().withdraw()
    folder_selected = filedialog.askdirectory(title="Select Client Project Folder")
    if not folder_selected:
        print("No folder selected. Exiting.")
        return

    db_name = os.path.basename(folder_selected).lower().replace(' ', '_')
    print(f"\nTarget Account Database: '{db_name}'")

    if not validator.validate_project_structure(folder_selected):
        input("\nPlease correct the errors and press Enter to exit.")
        return

    root_conn = db_operations.create_connection(config.DB_HOST, config.DB_USER, config.DB_PASSWORD)
    if not root_conn: return

    # Handle global Associate Base Data (ABD)
    abd_folder_path = os.path.join(folder_selected, config.ABD_FOLDER_NAME)
    if os.path.exists(abd_folder_path):
        print("\n--- Processing Global Associate Base Data ---")
        db_operations.create_database(root_conn, config.ABD_DB_NAME)
        abd_conn = db_operations.create_connection(config.DB_HOST, config.DB_USER, config.DB_PASSWORD,
                                                   config.ABD_DB_NAME)
        if abd_conn:
            db_operations.create_abd_table(abd_conn)
            db_operations.import_abd_data(abd_conn, abd_folder_path)
            abd_conn.close()
            print(f"Connection to global ABD database '{config.ABD_DB_NAME}' closed.")

    # Handle global PMR database
    print("\n--- Processing Global PMR Data ---")
    db_operations.create_database(root_conn, config.PMR_DB_NAME)
    pmr_conn = db_operations.create_connection(config.DB_HOST, config.DB_USER, config.DB_PASSWORD, config.PMR_DB_NAME)
    if not pmr_conn:
        root_conn.close()
        return

    pmr_files = [os.path.join(folder_selected, f) for f in os.listdir(folder_selected) if
                 f.startswith("PMR_") and f.endswith(".xlsx")]
    if pmr_files:
        db_operations.create_pmr_table(pmr_conn)
        db_operations.import_pmr_data(pmr_conn, pmr_files)

    pmr_conn.close()
    print(f"Connection to global PMR database '{config.PMR_DB_NAME}' closed.")

    # Connect to the account-specific database for processing
    print("\n--- Processing Account Specific Data ---")
    db_operations.create_database(root_conn, db_name)
    conn = db_operations.create_connection(config.DB_HOST, config.DB_USER, config.DB_PASSWORD, db_name)
    if not conn:
        root_conn.close()
        return

    db_operations.create_account_tables(conn)

    year_folders = [d for d in os.listdir(folder_selected) if
                    os.path.isdir(os.path.join(folder_selected, d)) and d.isdigit()]

    for fiscal_year in sorted(year_folders):
        year_path = os.path.join(folder_selected, fiscal_year)
        print(f"\n{'=' * 10} Processing Fiscal Year: {fiscal_year} {'=' * 10}")

        regional_file = os.path.join(year_path, config.REGIONAL_FILENAME)
        salary_file = os.path.join(year_path, config.SALARY_FILENAME)
        log_file = os.path.join(year_path, config.LOG_FILENAME)

        if os.path.exists(regional_file):
            preprocess_regional_file(regional_file)
            db_operations.import_regional_details(conn, regional_file, fiscal_year)

        if os.path.exists(salary_file):
            db_operations.import_salary_data(conn, salary_file, fiscal_year)

        db_operations.consolidate_data(conn, log_file, fiscal_year)

        # --- ADDED: Call the new function to backfill emails ---
        db_operations.fill_missing_emails(conn, db_name, fiscal_year)

    if year_folders:
        output_excel_path = os.path.join(folder_selected, f"{db_name}_final_report.xlsx")
        exporter.generate_final_report(conn, output_excel_path)

    conn.close()
    root_conn.close()
    print("\n✅ All fiscal years processed successfully.")


if __name__ == "__main__":
    main()