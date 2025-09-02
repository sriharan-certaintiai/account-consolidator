import mysql.connector

# --- Connection Details (Please update with your database credentials) ---
config = {
    'user': 'root',
    'password': 'root',
    'host': 'localhost',
    'database': 'global_abd_data_new'
}

# --- Column and Table Definitions ---
# List of tables to check, based on your image
tables_to_check = [
    "01_2024", "01_2025", "02_2025", "03_2024", "03_2025", "04_2023",
    "04_2024", "05_2023", "05_2024", "06_2023", "06_2024", "07_2023",
    "07_2024", "08_2024", "09_2023", "09_2024", "10_2023", "10_2024",
    "11_2024", "12_2024"
]

correct_column_name = 'technicalbsgsalessupport'
incorrect_column_name = 'technicalbsgsupport'

try:
    # Establish the database connection
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    print("Successfully connected to the database.")

    # Loop through each table provided in the list
    for table_name in tables_to_check:
        try:
            # Check if the table exists first
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if not cursor.fetchone():
                print(f"Table '{table_name}' not found. Skipping.")
                continue

            # Get the list of columns for the current table
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = [row[0].lower() for row in cursor.fetchall()]

            # Check if the correct column name already exists
            if correct_column_name in columns:
                print(f"Table '{table_name}' already has the correct column '{correct_column_name}'. No action needed.")
            # If not, check if the incorrect column name exists
            elif incorrect_column_name in columns:
                print(f"Found incorrect column '{incorrect_column_name}' in table '{table_name}'. Renaming...")

                # Prepare and execute the RENAME COLUMN statement for MySQL
                rename_sql = f"ALTER TABLE `{table_name}` RENAME COLUMN `{incorrect_column_name}` TO `{correct_column_name}`"

                cursor.execute(rename_sql)
                print(f"Successfully renamed column in table '{table_name}'.")
            else:
                print(
                    f"Table '{table_name}' does not have '{correct_column_name}' or '{incorrect_column_name}'. No action needed.")

        except mysql.connector.Error as err:
            print(f"An error occurred while processing table '{table_name}': {err}")

except mysql.connector.Error as err:
    print(f"Database connection error: {err}")
    print("Please ensure your database is running and the credentials in the script are correct.")

finally:
    # Close the connection if it was successfully opened
    if 'cnx' in locals() and cnx.is_connected():
        cursor.close()
        cnx.close()
        print("Database connection closed.")
