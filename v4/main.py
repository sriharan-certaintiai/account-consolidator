import pandas as pd
import configparser
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
import sys


def create_db_engine(config, db_name):
    """Creates a SQLAlchemy engine for a given database."""
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}/{db_name}"
        )
        connection = engine.connect()
        connection.close()
        return engine
    except Exception as e:
        print(f"❌ Error connecting to database '{db_name}': {e}")
        sys.exit(1)


def get_abd_data(main_df, abd_engine, abd_db_name):
    """
    Fetches data from the dynamic 'MM_YYYY' ABD tables.
    """
    print("-> Processing and fetching data from ABD tables...")
    main_df['abd_table_name'] = main_df['utilization_end_dt'].dt.strftime('%m_%Y')

    all_abd_data = []

    for name, group in main_df.groupby('abd_table_name'):
        table_name = name
        print(f"  - Querying ABD table: `{table_name}` for {len(group)} records.")
        try:
            query = f"""
                SELECT 
                    emplid, project_id, job_code_description, band, 
                    technicalbsgsalessupport, project_type_desc, project_pricing_type
                FROM `{abd_db_name}`.`{table_name}`
            """
            abd_month_df = pd.read_sql(query, abd_engine)
            abd_month_df['emplid'] = abd_month_df['emplid'].astype(str).str.strip()
            abd_month_df['project_id'] = abd_month_df['project_id'].astype(str).str.strip()

            merged_group = pd.merge(group, abd_month_df, on=['emplid', 'project_id'], how='left')
            all_abd_data.append(merged_group)

        except ProgrammingError:
            print(f"  ⚠️ Warning: Table `{table_name}` not found. Keeping original records with NULLs for ABD fields.")
            abd_columns = ['job_code_description', 'band', 'technicalbsgsalessupport', 'project_type_desc',
                           'project_pricing_type']
            for col in abd_columns:
                group[col] = None
            all_abd_data.append(group)
        except Exception as e:
            print(f"  ❌ Error processing table `{table_name}`: {e}")

    if not all_abd_data:
        print("❌ Error: No ABD data could be fetched. Aborting.")
        sys.exit(1)

    return pd.concat(all_abd_data, ignore_index=True)


def main():
    """Main function to run the consolidation process."""
    print("🚀 Starting data consolidation process...")

    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = config['database']
    tbl_config = config['tables']

    print("⚙️ Establishing database connections...")
    uk_engine = create_db_engine(db_config, db_config['uk_db'])
    abd_engine = create_db_engine(db_config, db_config['abd_db'])
    pmr_engine = create_db_engine(db_config, db_config['pmr_db'])

    print("📚 Reading source tables into memory...")
    try:
        df_regional = pd.read_sql(tbl_config['regional'], uk_engine, parse_dates=['utilization_end_dt'])
        print(f"  - Found {len(df_regional)} records in '{tbl_config['regional']}'.")

        df_salary = pd.read_sql(tbl_config['salary'], uk_engine)
        print(f"  - Found {len(df_salary)} records in '{tbl_config['salary']}'.")

        # --- NEW: Filter out NULL gross_pay records from salary ---
        initial_salary_count = len(df_salary)
        df_salary.dropna(subset=['gross_pay'], inplace=True)
        final_salary_count = len(df_salary)
        removed_count = initial_salary_count - final_salary_count
        if removed_count > 0:
            print(f"  - Removed {removed_count} records from salary where gross_pay was NULL.")
        # --------------------------------------------------------

        df_pmr = pd.read_sql(tbl_config['pmr_managers'], pmr_engine)
        print(f"  - Found {len(df_pmr)} records in '{tbl_config['pmr_managers']}'.")

    except Exception as e:
        print(f"❌ Error reading source tables: {e}")
        sys.exit(1)

    print("🧹 Preparing and cleaning all data for reliable merges...")

    df_regional['emplid'] = df_regional['emplid'].astype(str).str.strip()
    df_regional['project_id'] = df_regional['project_id'].astype(str).str.strip()
    df_regional['join_period'] = df_regional['utilization_end_dt'].dt.strftime('%Y-%m')

    df_salary['emplid'] = df_salary['emplid'].astype(str).str.strip()
    df_salary['join_period'] = pd.to_datetime(df_salary['month']).dt.strftime('%Y-%m')

    df_pmr = df_pmr.rename(columns={'PROJECT_ID': 'project_id'})
    df_pmr['project_id'] = df_pmr['project_id'].astype(str).str.strip()

    print("🔗 Performing data merges...")

    df_merged = get_abd_data(df_regional, abd_engine, db_config['abd_db'])

    print("-> Merging with Salary data...")
    df_merged = pd.merge(
        df_merged,
        df_salary[['emplid', 'join_period', 'gross_pay']],
        on=['emplid', 'join_period'],
        how='left'
    )

    print("-> Merging with Project Manager (PMR) data...")
    df_merged = pd.merge(
        df_merged,
        df_pmr,
        on='project_id',
        how='left'
    )

    print("📊 Finalizing columns for the consolidated table...")

    df_merged['Month'] = pd.to_datetime(df_merged['utilization_end_dt']).dt.to_period('M').dt.to_timestamp()

    df_final = df_merged.rename(columns={
        'emplid': 'EMPLID',
        'gross_pay': 'GROSS_PAY',
        'job_code_description': 'JOB_CODE_DESCRIPTION',
        'band': 'BAND',
        'technicalbsgsalessupport': 'TECHNICALBSGSALESSUPPORT',
        'current_work_location': 'CURRENT_WORK_LOCATION',
        'project_id': 'PROJECT_ID',
        'project_description': 'PROJECT_DESCRIPTION',
        'project_type_desc': 'PROJECT_TYPE_DESC',
        'project_pricing_type': 'PROJECT_PRICING_TYPE',
        'contract_type': 'CONTRACT_TYPE',
        'cust_name': 'CUST_NAME',
        'PGM_MANAGER_NAME': 'PGM_MANAGER_NAME',
        'PGM_MANAGER_EMAIL': 'PGM_MANAGER_EMAIL'
    })

    final_columns = [
        'EMPLID', 'Month', 'GROSS_PAY', 'JOB_CODE_DESCRIPTION', 'BAND',
        'TECHNICALBSGSALESSUPPORT', 'CURRENT_WORK_LOCATION', 'PROJECT_ID',
        'PROJECT_DESCRIPTION', 'PROJECT_TYPE_DESC', 'PROJECT_PRICING_TYPE',
        'CONTRACT_TYPE', 'CUST_NAME', 'PGM_MANAGER_NAME', 'PGM_MANAGER_EMAIL'
    ]
    df_final = df_final[final_columns]

    print(f"💾 Writing {len(df_final)} records to the new table `{tbl_config['consolidated']}`...")
    try:
        df_final.to_sql(
            tbl_config['consolidated'], uk_engine, if_exists='replace', index=False
        )
        print("✅ Successfully wrote data to the consolidated table.")
    except Exception as e:
        print(f"❌ Error writing to the database: {e}")
        sys.exit(1)

    print("\n🔍 Running validation checks...")
    try:
        consolidated_count = pd.read_sql(f"SELECT COUNT(*) FROM {tbl_config['consolidated']}", uk_engine).iloc[0, 0]
        regional_count = len(df_regional)

        consolidated_gross_pay = \
        pd.read_sql(f"SELECT SUM(GROSS_PAY) FROM {tbl_config['consolidated']}", uk_engine).iloc[0, 0]
        # For validation, we now need to sum the filtered salary table
        original_gross_pay = df_salary['gross_pay'].sum()

        print("\n--- Validation Report ---")
        print(f"Row Count in regional: {regional_count}")
        print(f"Row Count in consolidated: {consolidated_count}")
        print("-------------------------")
        print(f"Total Gross Pay in original salary table (after filtering NULLs): {original_gross_pay:,.2f}")
        print(f"Total Gross Pay in new consolidated table: {consolidated_gross_pay or 0:,.2f}")
        print("-------------------------")

        if regional_count == consolidated_count:
            print("✔️ Row count validation PASSED.")
        else:
            print("❌ Row count validation FAILED.")

        if not pd.isna(consolidated_gross_pay) and abs(consolidated_gross_pay - original_gross_pay) < 0.01:
            print("✔️ Gross pay validation PASSED.")
        else:
            print(
                "❌ Gross pay validation FAILED. (Note: A small difference is expected if not all regional records have a salary entry).")

    except Exception as e:
        print(f"❌ Could not run validation checks: {e}")
    finally:
        print("\n🎉 Consolidation process finished.")


if __name__ == '__main__':
    main()