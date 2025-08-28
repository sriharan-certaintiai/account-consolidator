# config.py

# --- DATABASE CONFIGURATION ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "root"
PMR_DB_NAME = "global_pmr_data"
ABD_DB_NAME = "global_associate_base_data" # <-- ADD THIS

# --- TABLE NAMES ---
REGIONAL_TABLE = "employee_regional_details"
SALARY_TABLE = "france_salary_data"
CONSOLIDATION_TABLE = "consolidation"
PMR_TABLE = "pmr_project_managers"
ABD_TABLE_NAME = "associate_base_data" # <-- ADD THIS

# --- FILE & FOLDER NAMES ---
REGIONAL_FILENAME = "Regional.xlsx"
SALARY_FILENAME = "Salary.xlsx"
LOG_FILENAME = "missing_projects.log"
ABD_FOLDER_NAME = "ABD" # <-- ADD THIS

# --- EXCEL SCHEMA VALIDATION & MAPPING ---
REGIONAL_COLUMNS = [
    'EMPLID', 'CURRENT WORK LOCATION', 'PROJECT ID', 'PROJECT DESCRIPTION',
    'PROJECT TYPE', 'CONTRACT TYPE', 'CUST NAME', 'RUS STATUS',
    'TOTAL HOURS', 'UTILIZATION END DT'
]
SALARY_COLUMNS = [
    'EMPLID', 'MONTH', 'GROSS PAY', 'DESIGNATION', 'BAND', 'FUNCTION'
]
PMR_COLUMNS = [
    'SAP PROJECT ID', 'PROGRAM MANAGER NAME', 'PROGRAM MANAGER EMAIL ID'
]
# --- ADD THIS SECTION for ABD column mapping ---
ABD_COLUMN_MAP = {
    'EMPLID': 'EMPLID',
    'TECHNICAL/BSG/SUPPORT': 'Function',
    'JOB_CODE_DESCRIPTION': 'Designation',
    'BAND': 'BAND'
}