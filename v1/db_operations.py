# db_operations.py

import re
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import calendar
import config
from tqdm import tqdm


def create_connection(host_name, user_name, user_password, db_name=None):
    try:
        connection = mysql.connector.connect(
            host=host_name, user=user_name, passwd=user_password, database=db_name if db_name else None
        )
        print(f"MySQL connection successful ({'DB: ' + db_name if db_name else 'server'})")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred")
        return None


def create_database(connection, db_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' created or already exists.")
    except Error as e:
        print(f"Error creating database {db_name}: {e}")


# --- CHANGE: Function to create only the PMR table ---
def create_pmr_table(connection):
    """Creates the PMR table in the global PMR database."""
    cursor = connection.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.PMR_TABLE} (
            PROJECT_ID VARCHAR(255) PRIMARY KEY,
            PGM_MANAGER_NAME VARCHAR(255),
            PGM_MANAGER_EMAIL VARCHAR(255)
        );
    """)
    print(f"Table '{config.PMR_TABLE}' is ready in the global PMR database.")


# --- CHANGE: Renamed function to reflect its purpose ---
def create_account_tables(connection):
    """Creates the tables specific to an account database."""
    cursor = connection.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.REGIONAL_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY, fiscal_year VARCHAR(10),
            EMPLID VARCHAR(255), CURRENT_WORK_LOCATION VARCHAR(255), 
            PROJECT_ID VARCHAR(255), PROJECT_DESCRIPTION TEXT, 
            PROJECT_TYPE VARCHAR(255), CONTRACT_TYPE VARCHAR(255), 
            CUST_NAME VARCHAR(255), RUS_STATUS VARCHAR(255), 
            TOTAL_HOURS DECIMAL(10, 2), Month DATE
        );
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.SALARY_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY, fiscal_year VARCHAR(10),
            EMPLID VARCHAR(255), MONTH DATE, GROSS_PAY DECIMAL(10, 2), 
            DESIGNATION VARCHAR(255), BAND VARCHAR(255), `FUNCTION` VARCHAR(255)
        );
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.CONSOLIDATION_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY, fiscal_year VARCHAR(10),
            EMPLID VARCHAR(255), Month DATE, GROSS_PAY DECIMAL(10, 2), 
            DESIGNATION VARCHAR(255), BAND VARCHAR(255), `FUNCTION` VARCHAR(255),
            CURRENT_WORK_LOCATION VARCHAR(255), PROJECT_ID VARCHAR(255), 
            PROJECT_DESCRIPTION TEXT, PROJECT_TYPE VARCHAR(255), 
            CONTRACT_TYPE VARCHAR(255), CUST_NAME VARCHAR(255),
            PGM_MANAGER_NAME VARCHAR(255), PGM_MANAGER_EMAIL VARCHAR(255),
            UNIQUE KEY `unique_employee_month_project_year` (`EMPLID`(100),`Month`,`PROJECT_ID`(100),`fiscal_year`)
        );
    """)
    print("All account-specific tables are ready.")


# --- CHANGE: Logic updated to IGNORE duplicates and not TRUNCATE ---
def import_pmr_data(connection, pmr_files):
    cursor = connection.cursor()
    # cursor.execute(f"TRUNCATE TABLE {config.PMR_TABLE}") # <-- REMOVED to make data persistent

    pmr_df_list = [pd.read_excel(file) for file in pmr_files]
    df_all_pmr = pd.concat(pmr_df_list, ignore_index=True)
    df_all_pmr.columns = df_all_pmr.columns.str.strip().str.upper()

    for _, row in tqdm(df_all_pmr.iterrows(), total=len(df_all_pmr), desc="Loading PMR data      "):
        stripped_id = str(row.get('SAP PROJECT ID', '')).strip()
        final_project_id = stripped_id.lstrip('0') if stripped_id.isdigit() else stripped_id

        if final_project_id:
            manager_name = str(row.get('PROGRAM MANAGER NAME', '')).strip()
            manager_email = str(row.get('PROGRAM MANAGER EMAIL ID', '')).strip()
            # --- CHANGE: Use INSERT IGNORE to skip existing Project IDs ---
            sql = f"""
                INSERT IGNORE INTO {config.PMR_TABLE} (PROJECT_ID, PGM_MANAGER_NAME, PGM_MANAGER_EMAIL)
                VALUES (%s, %s, %s)
            """
            cursor.execute(sql, (final_project_id, manager_name, manager_email))

    connection.commit()
    print("✅ PMR data loaded successfully (new entries added, existing entries ignored).")


def import_regional_details(connection, excel_path, fiscal_year):
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {config.REGIONAL_TABLE} WHERE fiscal_year = %s", (fiscal_year,))

    xls = pd.ExcelFile(excel_path)
    sheet_name_pattern = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2}$")
    sheets_to_process = [s for s in xls.sheet_names if sheet_name_pattern.match(s)]

    for sheet_name in sheets_to_process:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df.columns = [col.strip().upper() for col in df.columns]
        agg_rules = {'TOTAL HOURS': 'sum', 'CURRENT WORK LOCATION': 'first', 'PROJECT DESCRIPTION': 'first',
                     'PROJECT TYPE': 'first', 'CONTRACT TYPE': 'first', 'CUST NAME': 'first', 'RUS STATUS': 'first'}
        df_agg = df.groupby(['EMPLID', 'PROJECT ID'], as_index=False).agg(agg_rules)
        parsed_date = datetime.strptime(sheet_name, '%b-%y')
        _, num_days = calendar.monthrange(parsed_date.year, parsed_date.month)
        end_of_month_date = parsed_date.replace(day=num_days).date()

        for _, row in tqdm(df_agg.iterrows(), total=len(df_agg), desc=f"Loading regional {sheet_name}", leave=False):
            sql = f"INSERT INTO {config.REGIONAL_TABLE} (fiscal_year, EMPLID, CURRENT_WORK_LOCATION, PROJECT_ID, PROJECT_DESCRIPTION, PROJECT_TYPE, CONTRACT_TYPE, CUST_NAME, RUS_STATUS, TOTAL_HOURS, Month) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (fiscal_year, str(row.get('EMPLID', '')).strip(),
                      str(row.get('CURRENT WORK LOCATION', '')).strip(), str(row.get('PROJECT ID', '')).strip(),
                      str(row.get('PROJECT DESCRIPTION', '')).strip(), str(row.get('PROJECT TYPE', '')).strip(),
                      str(row.get('CONTRACT TYPE', '')).strip(), str(row.get('CUST NAME', '')).strip(),
                      str(row.get('RUS STATUS', '')).strip(), row.get('TOTAL HOURS'), end_of_month_date)
            cursor.execute(sql, values)

    connection.commit()
    print(f"Regional data for {fiscal_year} loaded into {config.REGIONAL_TABLE}")


def import_salary_data(connection, excel_path, fiscal_year):
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {config.SALARY_TABLE} WHERE fiscal_year = %s", (fiscal_year,))

    xls = pd.ExcelFile(excel_path)
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df.columns = df.columns.str.strip().str.upper()
        for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Loading salary {sheet_name: <15}", leave=False):
            month_date = pd.to_datetime(row['MONTH'])
            end_of_month_date = (month_date + pd.offsets.MonthEnd(0)).date()
            sql = f"INSERT INTO {config.SALARY_TABLE} (fiscal_year, EMPLID, MONTH, GROSS_PAY, DESIGNATION, BAND, `FUNCTION`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            values = (fiscal_year, str(row.get('EMPLID', '')).strip(), end_of_month_date, row.get('GROSS PAY'),
                      str(row.get('DESIGNATION', '')).strip(), str(row.get('BAND', '')).strip(),
                      str(row.get('FUNCTION', '')).strip())
            cursor.execute(sql, values)

    connection.commit()
    print(f"Salary data for {fiscal_year} loaded into {config.SALARY_TABLE}")


# --- CHANGE: JOIN now points to the global PMR database ---
def consolidate_data(connection, log_file, fiscal_year):
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {config.CONSOLIDATION_TABLE} WHERE fiscal_year = %s", (fiscal_year,))

    join_query = f"""
        INSERT INTO {config.CONSOLIDATION_TABLE} (
            fiscal_year, EMPLID, Month, GROSS_PAY, DESIGNATION, BAND, `FUNCTION`, 
            CURRENT_WORK_LOCATION, PROJECT_ID, PROJECT_DESCRIPTION, PROJECT_TYPE, 
            CONTRACT_TYPE, CUST_NAME, PGM_MANAGER_NAME, PGM_MANAGER_EMAIL
        )
        SELECT
            r.fiscal_year, r.EMPLID, r.Month, s.GROSS_PAY, s.DESIGNATION, s.BAND, s.`FUNCTION`,
            r.CURRENT_WORK_LOCATION, r.PROJECT_ID, r.PROJECT_DESCRIPTION,
            r.PROJECT_TYPE, r.CONTRACT_TYPE, r.CUST_NAME,
            pmr.PGM_MANAGER_NAME, pmr.PGM_MANAGER_EMAIL
        FROM {config.REGIONAL_TABLE} r
        LEFT JOIN {config.SALARY_TABLE} s ON r.EMPLID = s.EMPLID AND r.Month = s.Month
        LEFT JOIN {config.PMR_DB_NAME}.{config.PMR_TABLE} pmr ON r.PROJECT_ID = pmr.PROJECT_ID
        WHERE r.fiscal_year = %s
    """
    print(f"Consolidating data for {fiscal_year} via SQL join...")
    cursor.execute(join_query, (fiscal_year,))
    connection.commit()
    print(f"Data for {fiscal_year} consolidated.")

    missing_query = f"""
        SELECT DISTINCT r.PROJECT_ID
        FROM {config.REGIONAL_TABLE} r
        LEFT JOIN {config.PMR_DB_NAME}.{config.PMR_TABLE} pmr ON r.PROJECT_ID = pmr.PROJECT_ID
        WHERE r.fiscal_year = %s AND pmr.PROJECT_ID IS NULL 
        AND r.PROJECT_ID IS NOT NULL AND r.PROJECT_ID != ''
    """
    cursor.execute(missing_query, (fiscal_year,))
    missing_projects = [row[0] for row in cursor.fetchall()]
    with open(log_file, "w") as log:
        log.write(f"Missing Project IDs for {fiscal_year} (not found in PMR table):\n")
        if missing_projects:
            log.write("\n".join(sorted(missing_projects)))
    print(f"Missing projects for {fiscal_year} logged in {log_file}.")


def create_abd_table(connection):
    """Creates the associate base data table in its own database."""
    cursor = connection.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.ABD_TABLE_NAME} (
            EMPLID VARCHAR(255) PRIMARY KEY,
            `Function` VARCHAR(255),
            Designation VARCHAR(255),
            BAND VARCHAR(255)
        );
    """)
    print(f"Table '{config.ABD_TABLE_NAME}' is ready in the global ABD database.")


# --- NEW FUNCTION to import and process ABD data ---
def import_abd_data(connection, abd_folder_path):
    """
    Finds all ABD files, extracts data from 'Base' sheets, processes it,
    and loads the unique, latest records into the database.
    """
    cursor = connection.cursor()
    all_dfs = []

    abd_files = [f for f in os.listdir(abd_folder_path) if f.endswith(('.xlsx', '.xls'))]
    print(f"Found {len(abd_files)} files in the ABD folder.")

    for file_name in tqdm(abd_files, desc="Processing ABD files"):
        file_path = os.path.join(abd_folder_path, file_name)
        try:
            xls = pd.ExcelFile(file_path)
            target_sheet = None
            for sheet_name in xls.sheet_names:
                if 'base' in sheet_name.lower():
                    target_sheet = sheet_name
                    break

            if target_sheet:
                df = pd.read_excel(xls, sheet_name=target_sheet)
                df.columns = [str(col).strip().upper() for col in df.columns]

                # Filter for only the columns we need
                required_cols = list(config.ABD_COLUMN_MAP.keys())
                existing_cols = [col for col in required_cols if col in df.columns]

                if 'EMPLID' in existing_cols:
                    df_filtered = df[existing_cols]
                    # Rename columns to match DB schema
                    df_renamed = df_filtered.rename(columns=config.ABD_COLUMN_MAP)
                    all_dfs.append(df_renamed)

        except Exception as e:
            print(f"Warning: Could not process file {file_name}. Error: {e}")

    if not all_dfs:
        print("No valid ABD data found to process.")
        return

    # Combine all data and handle duplicates
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.dropna(subset=['EMPLID'], inplace=True)  # Ensure EMPLID is not null
    combined_df['EMPLID'] = combined_df['EMPLID'].astype(str)

    # Keep the last entry for each EMPLID, ensuring we have the latest data
    final_df = combined_df.drop_duplicates(subset=['EMPLID'], keep='last')

    # Load data into the database
    cursor.execute(f"TRUNCATE TABLE {config.ABD_TABLE_NAME}")

    for _, row in tqdm(final_df.iterrows(), total=len(final_df), desc="Loading ABD data      "):
        sql = f"""
            INSERT INTO {config.ABD_TABLE_NAME} (EMPLID, `Function`, Designation, BAND)
            VALUES (%s, %s, %s, %s)
        """
        values = (
            row.get('EMPLID'),
            row.get('Function'),
            row.get('Designation'),
            row.get('BAND')
        )
        cursor.execute(sql, values)

    connection.commit()
    print(f"✅ {len(final_df)} unique associate records loaded into global ABD database.")
