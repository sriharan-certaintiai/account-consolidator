# file_preprocessor.py
import pandas as pd
from tqdm import tqdm

def preprocess_regional_file(file_path):
    print(f"Preprocessing {file_path} ...")
    try:
        df = pd.read_excel(file_path)
        def clean_project_id(pid):
            stripped_id = str(pid).strip()
            return stripped_id.lstrip('0') if stripped_id.isdigit() else stripped_id
        if 'PROJECT ID' in df.columns: df['PROJECT ID'] = df['PROJECT ID'].apply(clean_project_id)
        df['UTILIZATION END DT'] = pd.to_datetime(df['UTILIZATION END DT'], errors='coerce')
        unique_dates = sorted(df['UTILIZATION END DT'].dropna().unique())
        with pd.ExcelWriter(file_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            for date_val in tqdm(unique_dates, desc="Creating pivot sheets"):
                filtered_df = df[df['UTILIZATION END DT'] == date_val]
                pivot = pd.pivot_table(filtered_df, index=['EMPLID','CURRENT WORK LOCATION','PROJECT ID', 'PROJECT DESCRIPTION', 'PROJECT TYPE', 'CONTRACT TYPE', 'CUST NAME', 'RUS STATUS'], values='TOTAL HOURS', aggfunc='sum', fill_value=0).reset_index()
                pivot = pivot.sort_values(by=['EMPLID', 'RUS STATUS', 'TOTAL HOURS'], ascending=[True, True, False])
                writer.sheets.pop(pd.to_datetime(date_val).strftime("%b-%y"), None) # Remove sheet if it exists
                pivot.to_excel(writer, sheet_name=pd.to_datetime(date_val).strftime("%b-%y"), index=False)
    except Exception as e: print(f"An error occurred during preprocessing: {e}")