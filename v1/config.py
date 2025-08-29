# config.py

# --- DATABASE CONFIGURATION ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "root"
PMR_DB_NAME = "global_pmr_data"
ABD_DB_NAME = "global_associate_base_data"

# --- TABLE NAMES ---
REGIONAL_TABLE = "employee_regional_details"
SALARY_TABLE = "salary_data"
CONSOLIDATION_TABLE = "consolidation"
PMR_TABLE = "pmr_project_managers"
ABD_TABLE_NAME = "associate_base_data"

# --- FILE & FOLDER NAMES ---
REGIONAL_FILENAME = "Regional.xlsx"
SALARY_FILENAME = "Salary.xlsx"
LOG_FILENAME = "missing_projects.log"
ABD_FOLDER_NAME = "ABD"

# --- EXCEL SCHEMA VALIDATION & MAPPING ---
REGIONAL_COLUMNS = [
    'EMPLID', 'CURRENT WORK LOCATION', 'PROJECT ID', 'PROJECT DESCRIPTION',
    'PROJECT TYPE', 'CONTRACT TYPE', 'CUST NAME', 'RUS STATUS',
    'TOTAL HOURS', 'UTILIZATION END DT'
]
SALARY_COLUMNS = [
    'EMPLID', 'MONTH', 'GROSS PAY'
]
PMR_COLUMNS = [
    'SAP PROJECT ID', 'PROGRAM MANAGER NAME', 'PROGRAM MANAGER EMAIL ID'
]
ABD_COLUMN_MAP = {
    'EMPLID': 'EMPLID',
    'TECHNICAL/': 'Function', # <-- THIS IS THE CHANGED LINE
    'JOB_CODE_DESCRIPTION': 'Designation',
    'BAND': 'BAND',
    'PROGRAM_MANAGER_NAME': 'PROGRAM_MANAGER_NAME'
}