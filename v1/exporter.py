# exporter.py

import pandas as pd
import config


def generate_final_report(connection, output_excel_path):
    """
    Queries the database for the final consolidated data and anomalies,
    then exports them to a formatted Excel file with two sheets.
    """
    print("\nGenerating final Excel report...")

    try:
        # --- Query 1: Get all consolidated data ---
        consolidation_query = f"SELECT * FROM {config.CONSOLIDATION_TABLE} ORDER BY fiscal_year, Month"
        df_consolidation = pd.read_sql(consolidation_query, connection)
        print(f"  - Found {len(df_consolidation)} rows for the consolidation sheet.")

        # --- Query 2: Get anomalies (missing PMR details) ---
        anomalies_query = f"""
            SELECT DISTINCT 
                fiscal_year, 
                PROJECT_ID 
            FROM {config.CONSOLIDATION_TABLE} 
            WHERE PGM_MANAGER_NAME IS NULL 
              AND PROJECT_ID IS NOT NULL 
              AND PROJECT_ID != '' 
            ORDER BY fiscal_year, PROJECT_ID
        """
        df_anomalies = pd.read_sql(anomalies_query, connection)
        print(f"  - Found {len(df_anomalies)} unique project IDs with missing manager details.")

        # --- Write to Excel ---
        with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
            df_consolidation.to_excel(writer, sheet_name='Consolidated_Data', index=False)
            df_anomalies.to_excel(writer, sheet_name='Anomalies_Missing_PMR', index=False)

            # --- CORRECTION START ---
            # Correctly auto-fit column widths for both sheets

            # Helper function to calculate max width
            def get_col_widths(dataframe):
                return [max([len(str(s)) for s in dataframe[col].values] + [len(col)]) + 2 for col in dataframe.columns]

            # Apply formatting to the 'Consolidated_Data' sheet
            worksheet_consolidation = writer.sheets['Consolidated_Data']
            for i, width in enumerate(get_col_widths(df_consolidation)):
                worksheet_consolidation.set_column(i, i, width)

            # Apply formatting to the 'Anomalies_Missing_PMR' sheet
            worksheet_anomalies = writer.sheets['Anomalies_Missing_PMR']
            for i, width in enumerate(get_col_widths(df_anomalies)):
                worksheet_anomalies.set_column(i, i, width)
            # --- CORRECTION END ---

        print(f"\n✅ Final report successfully saved to:\n{output_excel_path}")

    except Exception as e:
        print(f"\n❌ An error occurred while generating the Excel report: {e}")