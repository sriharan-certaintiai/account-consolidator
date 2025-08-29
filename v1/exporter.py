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
            WHERE PGM_MANAGER_EMAIL IS NULL 
              AND PROJECT_ID IS NOT NULL 
              AND PROJECT_ID != '' 
            ORDER BY fiscal_year, PROJECT_ID
        """
        df_anomalies = pd.read_sql(anomalies_query, connection)
        print(f"  - Found {len(df_anomalies)} unique project IDs with missing manager emails.")

        # --- Prepare the optional ER_NIC_SUM DataFrame ---
        has_er_nic_sum_data = 'ER_NIC_SUM' in df_consolidation.columns and df_consolidation['ER_NIC_SUM'].notna().any()

        df_er_nic = None
        if has_er_nic_sum_data:
            df_er_nic = df_consolidation[['fiscal_year', 'Month', 'EMPLID', 'GROSS_PAY', 'ER_NIC_SUM']].copy()
            df_er_nic.dropna(subset=['ER_NIC_SUM'], inplace=True)
            print(f"  - Found {len(df_er_nic)} rows with ER_NIC_SUM data for the new sheet.")

        # --- NEW: Create a version of the main DataFrame for export without the ER_NIC_SUM column ---
        df_consolidation_export = df_consolidation.copy()
        if 'ER_NIC_SUM' in df_consolidation_export.columns:
            df_consolidation_export = df_consolidation_export.drop(columns=['ER_NIC_SUM'])

        # --- Write to Excel with formatting ---
        with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
            # Write the main sheets, using the modified DataFrame for consolidation
            df_consolidation_export.to_excel(writer, sheet_name='Consolidated_Data', index=False)
            df_anomalies.to_excel(writer, sheet_name='Anomalies_Missing_PMR', index=False)

            # Conditionally write the ER_NIC_SUM sheet
            if has_er_nic_sum_data and not df_er_nic.empty:
                df_er_nic.to_excel(writer, sheet_name='ER_NIC_SUM_Details', index=False)

            # Helper function to calculate max width for columns
            def get_col_widths(dataframe):
                return [max([len(str(s)) for s in dataframe[col].values] + [len(col)]) + 2 for col in dataframe.columns]

            # Format the main sheets
            worksheet_consolidation = writer.sheets['Consolidated_Data']
            for i, width in enumerate(get_col_widths(df_consolidation_export)):
                worksheet_consolidation.set_column(i, i, width)

            worksheet_anomalies = writer.sheets['Anomalies_Missing_PMR']
            for i, width in enumerate(get_col_widths(df_anomalies)):
                worksheet_anomalies.set_column(i, i, width)

            # Conditionally format the new sheet
            if has_er_nic_sum_data and not df_er_nic.empty:
                worksheet_er_nic = writer.sheets['ER_NIC_SUM_Details']
                for i, width in enumerate(get_col_widths(df_er_nic)):
                    worksheet_er_nic.set_column(i, i, width)

        print(f"\n✅ Final report successfully saved to:\n{output_excel_path}")

    except Exception as e:
        print(f"\n❌ An error occurred while generating the Excel report: {e}")