# main.py

import os
from tkinter import Tk, filedialog
import config
import db_operations
import validator
from file_preprocessor import preprocess_regional_file

def main():
    """Main function to drive the data processing pipeline."""
    Tk().withdraw()
    folder_selected = filedialog.askdirectory(title="Select Report Folder")
    if not folder_selected:
        print("No folder selected. Exiting.")
        return

    if not validator.validate_project_structure(folder_selected):
        input("\nPlease correct the errors and press Enter to exit.")
        return

    root_conn = db_operations.create_connection(config.DB_HOST, config.DB_USER, config.DB_PASSWORD)
    if not root_conn:
        print("Could not connect to MySQL server. Exiting.")
        return

    db_operations.create_database(root_conn, config.COMMON_DB_NAME)
    common_conn = db_operations.create_connection(config.DB_HOST, config.DB_USER, config.DB_PASSWORD, config.COMMON_DB_NAME)
    if not common_conn:
        print(f"Could not connect to common DB '{config.COMMON_DB_NAME}'. Exiting.")
        root_conn.close()
        return
    
    pmr_files = [os.path.join(folder_selected, f) for f in os.listdir(folder_selected) if f.startswith("PMR_") and f.endswith(".xlsx")]
    if pmr_files:
        db_operations.create_pmr_table(common_conn)
        db_operations.import_pmr_data(common_conn, pmr_files)
    
    common_conn.close()
    print(f"Connection to '{config.COMMON_DB_NAME}' closed.")

    for fy_folder in os.listdir(folder_selected):
        fy_path = os.path.join(folder_selected, fy_folder)
        if os.path.isdir(fy_path) and fy_folder.startswith("FY"):
            print(f"\n{'='*10} Processing {fy_folder} {'='*10}")
            
            db_operations.create_database(root_conn, fy_folder)
            conn = db_operations.create_connection(config.DB_HOST, config.DB_USER, config.DB_PASSWORD, fy_folder)
            if not conn:
                print(f"Skipping {fy_folder} due to connection failure.")
                continue

            db_operations.create_tables(conn)

            regional_file = os.path.join(fy_path, config.REGIONAL_FILENAME)
            salary_file = os.path.join(fy_path, config.SALARY_FILENAME)
            log_file = os.path.join(fy_path, config.LOG_FILENAME)

            if os.path.exists(regional_file):
                preprocess_regional_file(regional_file)
                db_operations.import_regional_details(conn, regional_file)
            
            if os.path.exists(salary_file):
                db_operations.import_salary_data(conn, salary_file)

            db_operations.consolidate_data(conn, log_file)
            
            conn.close()
            print(f"Connection to '{fy_folder}' closed.")

    root_conn.close()
    print("\n✅ All FY folders processed successfully.")

if __name__ == "__main__":
    main()