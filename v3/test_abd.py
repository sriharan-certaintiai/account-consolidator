import mysql.connector

# --- CONFIGURATION ---
# Use the same config as your main script
CONFIG = {
    'db_connection': {
        'host': 'localhost',
        'user': 'root',
        'password': 'root'
    },
    'db_names': {
        'global_abd': 'global_abd_data_new'
    }
}

def run_diagnostics():
    """
    Connects to the DB and tries to read the problematic column from each monthly table.
    """
    # These are the months your main script found.
    # We will test each one.
    months_to_test = [
        (2024, 4), (2024, 5), (2024, 6), (2024, 7), (2024, 8), (2024, 9),
        (2024, 10), (2024, 11), (2024, 12), (2025, 1), (2025, 2), (2025, 3)
    ]

    try:
        cnx = mysql.connector.connect(**CONFIG['db_connection'])
        cursor = cnx.cursor(dictionary=True)
        print("Successfully connected to MySQL for diagnostics.\n")
        db_global_abd = CONFIG['db_names']['global_abd']

        for year, month in months_to_test:
            abd_table_name = f"{month:02d}_{year}"
            print(f"--- Testing Table: `{abd_table_name}` ---")

            # First, check if the table exists
            cursor.execute(f"SHOW TABLES IN `{db_global_abd}` LIKE '{abd_table_name}'")
            if not cursor.fetchone():
                print("Result: FAILED - Table does not exist.\n")
                continue

            # If it exists, try to select the specific column
            try:
                test_query = f"""
                    SELECT
                        `emplid`,
                        `technicalbsgsgsupport`
                    FROM
                        `{db_global_abd}`.`{abd_table_name}`
                    LIMIT 1;
                """
                cursor.execute(test_query)
                result = cursor.fetchone()
                print(f"Result: SUCCESS - Column 'technicalbsgsgsupport' was found.")
                print(f"Sample Data: {result}\n")

            except mysql.connector.Error as err:
                print(f"Result: FAILED - Could not read the column.")
                print(f"MySQL Error: {err}\n")

    except mysql.connector.Error as err:
        print(f"A fatal database error occurred: {err}")
    finally:
        if 'cnx' in locals() and cnx.is_connected():
            cursor.close()
            cnx.close()
            print("MySQL connection is closed.")


if __name__ == '__main__':
    run_diagnostics()