# validator.py

import os
import pandas as pd
import config
from tqdm import tqdm


def _verify_excel_columns(file_path, expected_columns, sheet_name=0):
    """Checks if an Excel sheet contains all expected columns."""
    missing_cols = []
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        actual_columns = {col.strip().upper() for col in df.columns}
        for expected_col in expected_columns:
            if expected_col.upper() not in actual_columns:
                missing_cols.append(expected_col)
    except Exception as e:
        return [f"Could not read file or sheet: {e}"]
    return missing_cols


def validate_project_structure(root_folder):
    """Verifies the folder and file structure with a progress bar."""
    print("\n🔍 Starting validation process...")
    errors = []
    warnings = []

    validation_tasks = []
    pmr_files = [f for f in os.listdir(root_folder) if f.startswith("PMR_") and f.endswith(".xlsx")]
    year_folders = [d for d in os.listdir(root_folder) if os.path.isdir(os.path.join(root_folder, d)) and d.isdigit()]

    if not pmr_files:
        warnings.append("No 'PMR_*.xlsx' files found. Manager details will be missing.")
    else:
        validation_tasks.append({'type': 'pmr_schema', 'file': pmr_files[0]})

    if not year_folders:
        errors.append("CRITICAL: No yearly subfolders (e.g., '2023', '2024') found.")
    else:
        for folder in year_folders:
            validation_tasks.append({'type': 'file_check', 'folder': folder, 'filename': config.REGIONAL_FILENAME})
            validation_tasks.append({'type': 'file_check', 'folder': folder, 'filename': config.SALARY_FILENAME})

    with tqdm(total=len(validation_tasks), desc="Validating files") as pbar:
        for task in validation_tasks:
            pbar.set_postfix_str(f"Checking {task.get('file') or task.get('filename')}", refresh=True)

            if task['type'] == 'pmr_schema':
                pmr_path = os.path.join(root_folder, task['file'])
                missing = _verify_excel_columns(pmr_path, config.PMR_COLUMNS)
                if missing:
                    errors.append(f"In {task['file']}: Missing columns - {', '.join(missing)}")

            elif task['type'] == 'file_check':
                folder, filename = task['folder'], task['filename']
                file_path = os.path.join(root_folder, folder, filename)
                if not os.path.exists(file_path):
                    errors.append(f"In {folder}: File '{filename}' is missing.")
                else:
                    expected_cols = config.REGIONAL_COLUMNS if 'Regional' in filename else config.SALARY_COLUMNS
                    missing = _verify_excel_columns(file_path, expected_cols)
                    if missing:
                        errors.append(f"In {folder}/{filename}: Missing columns - {', '.join(missing)}")
            pbar.update(1)

    print("\n" + "=" * 25 + "\n   Validation Summary\n" + "=" * 25)
    if errors:
        print("\n❌ Validation Failed. Please fix the following critical errors:")
        for i, error in enumerate(errors, 1): print(f"  {i}. {error}")
        return False
    if warnings:
        print("\n⚠️ Validation Passed with Warnings:")
        for i, warning in enumerate(warnings, 1): print(f"  {i}. {warning}")
    print("\n✅ Validation Successful. All checks passed.")
    return True