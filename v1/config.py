# config.py

# --- DATABASE CONFIGURATION ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "root"

# --- TABLE NAMES ---
REGIONAL_TABLE = "employee_regional_details"
SALARY_TABLE = "france_salary_data"
CONSOLIDATION_TABLE = "consolidation"
PMR_TABLE = "pmr_project_managers"

# --- FILE & FOLDER NAMES ---
REGIONAL_FILENAME = "Regional.xlsx"
SALARY_FILENAME = "Salary.xlsx"
LOG_FILENAME = "missing_projects.log"

# --- EXCEL SCHEMA VALIDATION ---
# Columns that MUST exist in the source files. Case-insensitive.
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