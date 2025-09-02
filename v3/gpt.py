import mysql.connector
from mysql.connector import Error
import re
from collections import defaultdict
from datetime import datetime

"""
Consolidator v3
- Fixes GROSS_PAY duplication by allocating salary per (emplid, MM_YYYY)
- Modes: 'split_by_hours' | 'split_equal' | 'first_only'
- Robust month-key matching across uk.regional, uk.salary, and ABD tables (MM_YYYY)
- Debug validation prints to verify sums
"""

# =======================
# CONFIGURATION
# =======================
CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
}

DBS = {
    "uk": "uk",
    "abd": "global_abd_data_new",
    "pmr": "global_pmr_data",
}

TABLES = {
    "regional": "regional",
    "salary": "salary",
    "consolidated": "consolidated",
    "pmr": "pmr_project_managers",
}

# How to allocate salary across multiple regional rows for a given (emplid, month)
GROSS_PAY_ALLOCATION = "split_by_hours"  # 'split_by_hours' | 'split_equal' | 'first_only'
DEBUG = True

# =======================
# DB HELPERS
# =======================

def get_connection():
    return mysql.connector.connect(
        host=CONFIG["host"],
        user=CONFIG["user"],
        password=CONFIG["password"],
    )


def month_key_from_dt(dt):
    if not dt:
        return None
    if isinstance(dt, str):
        # try parse common formats
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                dt = datetime.strptime(dt, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    return dt.strftime("%m_%Y")


# =======================
# DATA LOADERS
# =======================

def get_abd_tables(cur):
    cur.execute(f"SHOW TABLES FROM {DBS['abd']}")
    return [r[0] for r in cur.fetchall() if re.match(r"^\d{2}_\d{4}$", r[0])]


def load_regional(cur):
    cur.execute(f"SELECT * FROM {DBS['uk']}.{TABLES['regional']}")
    rows = cur.fetchall(); cols = cur.column_names
    idx = {c: i for i, c in enumerate(cols)}
    return rows, idx


def load_salary(cur):
    cur.execute(f"SELECT * FROM {DBS['uk']}.{TABLES['salary']}")
    rows = cur.fetchall(); cols = cur.column_names
    idx = {c: i for i, c in enumerate(cols)}
    # Build map by (emplid, MM_YYYY)
    sal_map = {}
    for r in rows:
        emplid = r[idx["emplid"]]
        mkey = month_key_from_dt(r[idx["month"]])
        sal_map[(emplid, mkey)] = float(r[idx["gross_pay"]]) if r[idx["gross_pay"]] is not None else None
    return rows, idx, sal_map


def load_pmr(cur):
    cur.execute(f"SELECT * FROM {DBS['pmr']}.{TABLES['pmr']}")
    rows = cur.fetchall(); cols = cur.column_names
    idx = {c: i for i, c in enumerate(cols)}
    # Map by PROJECT_ID
    pmr_map = {}
    for r in rows:
        pmr_map[r[idx["PROJECT_ID"]]] = (
            r[idx.get("PGM_MANAGER_NAME")],
            r[idx.get("PGM_MANAGER_EMAIL")],
        )
    return rows, idx, pmr_map


def load_abd(cur):
    abd_tables = get_abd_tables(cur)
    abd_map = {}
    # For each MM_YYYY table, map (emplid, project_id, MM_YYYY) -> row dict
    for t in abd_tables:
        cur.execute(f"SELECT * FROM {DBS['abd']}.{t}")
        rows = cur.fetchall(); cols = cur.column_names
        idx = {c: i for i, c in enumerate(cols)}
        for r in rows:
            key = (r[idx.get("emplid")], r[idx.get("project_id")], t)
            abd_map[key] = {
                "job_code_description": r[idx.get("job_code_description")],
                "band": r[idx.get("band")],
                "technicalbsgsalessupport": r[idx.get("technicalbsgsalessupport")],
                "project_type_desc": r[idx.get("project_type_desc")],
                "project_pricing_type": r[idx.get("project_pricing_type")],
            }
    return abd_map


# =======================
# DDL
# =======================

def create_consolidated(cur):
    cur.execute(f"DROP TABLE IF EXISTS {DBS['uk']}.{TABLES['consolidated']}")
    cur.execute(
        f"""
        CREATE TABLE {DBS['uk']}.{TABLES['consolidated']} (
            emplid VARCHAR(50),
            month VARCHAR(20),
            gross_pay DOUBLE,
            job_code_description VARCHAR(255),
            band VARCHAR(50),
            technicalbsgsalessupport VARCHAR(255),
            current_work_location VARCHAR(255),
            project_id VARCHAR(50),
            project_description VARCHAR(255),
            project_type_desc VARCHAR(255),
            project_pricing_type VARCHAR(255),
            contract_type VARCHAR(255),
            cust_name VARCHAR(255),
            pgm_manager_name VARCHAR(255),
            pgm_manager_email VARCHAR(255)
        )
        """
    )


# =======================
# CORE LOGIC
# =======================

def allocate_salary(regional_rows, reg_idx, salary_map):
    """Return map of row_index -> allocated gross_pay so that total matches salary.
    Allocation is per (emplid, month_key).
    """
    # Group regional rows by (emplid, month_key)
    groups = defaultdict(list)
    for i, r in enumerate(regional_rows):
        emplid = r[reg_idx["emplid"]]
        mkey = month_key_from_dt(r[reg_idx["utilization_end_dt"]])
        groups[(emplid, mkey)].append(i)

    allocated = {i: None for i in range(len(regional_rows))}

    for (emplid, mkey), idxs in groups.items():
        sal = salary_map.get((emplid, mkey))
        if sal is None:
            # nothing to allocate
            continue
        if len(idxs) == 1 or GROSS_PAY_ALLOCATION == "first_only":
            # put all on the first row; others zero
            first = idxs[0]
            allocated[first] = sal
            if GROSS_PAY_ALLOCATION == "first_only":
                for j in idxs[1:]:
                    allocated[j] = 0.0
            else:
                # len==1 case
                pass
        elif GROSS_PAY_ALLOCATION == "split_equal":
            part = sal / float(len(idxs))
            for j in idxs:
                allocated[j] = part
        elif GROSS_PAY_ALLOCATION == "split_by_hours":
            # Pro-rate by total_hours
            hours = []
            total = 0.0
            th_idx = reg_idx.get("total_hours")
            for j in idxs:
                h = regional_rows[j][th_idx] if th_idx is not None else None
                h = float(h) if h is not None else 0.0
                hours.append(h)
                total += h
            if total > 0:
                for j, h in zip(idxs, hours):
                    allocated[j] = sal * (h / total)
            else:
                # fallback to equal split
                part = sal / float(len(idxs))
                for j in idxs:
                    allocated[j] = part
        else:
            # safety fallback
            part = sal / float(len(idxs))
            for j in idxs:
                allocated[j] = part
    return allocated


def consolidate():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Load all datasets
        regional_rows, reg_idx = load_regional(cur)
        salary_rows, sal_idx, salary_map = load_salary(cur)
        _, _, pmr_map = load_pmr(cur)
        abd_map = load_abd(cur)

        # Prepare consolidated table
        create_consolidated(cur)

        # Pre-compute gross pay allocation per regional row
        allocation = allocate_salary(regional_rows, reg_idx, salary_map)

        insert_sql = f"""
            INSERT INTO {DBS['uk']}.{TABLES['consolidated']} (
                emplid, month, gross_pay, job_code_description, band, technicalbsgsalessupport,
                current_work_location, project_id, project_description, project_type_desc,
                project_pricing_type, contract_type, cust_name, pgm_manager_name, pgm_manager_email
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        # Insert rows
        for i, r in enumerate(regional_rows):
            emplid = r[reg_idx["emplid"]]
            project_id = r[reg_idx.get("project_id")]
            mkey = month_key_from_dt(r[reg_idx["utilization_end_dt"]])

            # Salary allocated
            gross_pay = allocation.get(i)

            # ABD lookups
            abd_rec = abd_map.get((emplid, project_id, mkey), {})

            # PMR manager
            pmr_name, pmr_email = (None, None)
            if project_id in pmr_map:
                pmr_name, pmr_email = pmr_map[project_id]

            data = (
                emplid,
                mkey,
                gross_pay,
                abd_rec.get("job_code_description"),
                abd_rec.get("band"),
                abd_rec.get("technicalbsgsalessupport"),
                r[reg_idx.get("current_work_location")],
                project_id,
                r[reg_idx.get("project_description")],
                abd_rec.get("project_type_desc"),
                abd_rec.get("project_pricing_type"),
                r[reg_idx.get("contract_type")],
                r[reg_idx.get("cust_name")],
                pmr_name,
                pmr_email,
            )
            cur.execute(insert_sql, data)

        conn.commit()

        if DEBUG:
            # Validation: compare salary sum by month vs consolidated sum
            cur.execute(f"SELECT IFNULL(SUM(gross_pay),0) FROM {DBS['uk']}.{TABLES['consolidated']}")
            cons_sum = cur.fetchone()[0]
            cur.execute(f"SELECT IFNULL(SUM(gross_pay),0) FROM {DBS['uk']}.{TABLES['salary']}")
            sal_sum = cur.fetchone()[0]
            print(f"Validation - Total GROSS_PAY | consolidated: {cons_sum:.2f} vs salary: {sal_sum:.2f}")

    except Error as e:
        print("MySQL Error:", e)
    finally:
        if conn and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    consolidate()
