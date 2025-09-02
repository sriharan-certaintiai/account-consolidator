#!/usr/bin/env python3
"""
MySQL Consolidation Utility

Generates a consolidated table `uk.consolidated` by joining:
 - uk.regional
 - uk.salary
 - all month tables in global_abd_data_new (e.g. `01_2025`, `02_2025`, ...)
 - global_pmr_data.pmr_project_managers

How it works (high level):
 1. Reads config at the top of this file.
 2. Builds a UNION ALL of all ABD monthly tables into a temporary table `abd_combined`.
 3. Joins regional -> salary (by emplid + month) -> abd_combined (by emplid+project_id+month) -> pmr managers (by project_id).
 4. Inserts final rows into `uk.consolidated` (create or append based on config).

Edit the `CONFIG` dict below to match your environment and column-names.

Requirements:
    pip install mysql-connector-python

Run:
    python consolidate_mysql_utility.py --rebuild    # drop/create consolidated
    python consolidate_mysql_utility.py --dry-run    # show generated SQL and exit

Designed to be configurable and defensive: if an ABD table is missing some columns we select NULL for those columns.

"""

import argparse
import getpass
import logging
import re
import sys
from typing import Dict, List

import mysql.connector

# ---------------------------
# CONFIG - edit these values
# ---------------------------
CONFIG = {
    # MySQL connection
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "root",   # change or set to None to prompt
    },

    # Database names
    "databases": {
        "uk": "uk",
        "abd": "global_abd_data_new",
        "pmr": "global_pmr_data",
    },

    # Table names (if different change here)
    "tables": {
        "regional": "regional",
        "salary": "salary",
        "consolidated": "consolidated",
        "pmr_managers": "pmr_project_managers",
    },

    # Column mapping - change if your columns have different names
    "columns": {
        "regional": {
            "emplid": "emplid",
            "utilization_end_dt": "utilization_end_dt",
            "current_work_location": "current_work_location",  # or current_location_description
            "project_id": "project_id",
            "project_description": "project_description",
            "contract_type": "contract_type",
            "cust_name": "cust_name",
        },
        "salary": {
            "emplid": "emplid",
            "month": "month",  # datetime or date column
            "gross_pay": "gross_pay",
        },
        "abd": {
            # these are expected logical names; if the ABD tables have different column names
            # update accordingly. When missing, the script will insert NULLs for those values.
            "emplid": "emplid",
            "project_id": "project_id",
            "job_code_description": "job_code_description",
            "band": "band",
            "technicalbsgsalessupport": "technicalbsgsalessupport",
            "project_type_desc": "project_type_desc",
            "project_pricing_type": "project_pricing_type",
        },
        "pmr": {
            "project_id": "project_id",
            "manager_name": "manager_name",    # set to actual column name in pmr table
            "manager_email": "manager_email",
        },
    },

    # ABD table name pattern in abd DB: e.g. 01_2025, 02_2025... change regexp if yours differs
    "abd_table_pattern": r'^[0-9]{2}_[0-9]{4}$',

    # Consolidated table creation options
    "consolidated_options": {
        "rebuild_schema": True,   # if True and --rebuild passed, drop then create
        "primary_key": ["EMPLID", "Month", "PROJECT_ID"],
    },

    # Behavior flags
    "behavior": {
        "dry_run": False,  # set True to print SQL and exit
    },
}

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------
# Helper functions
# ---------------------------

def quote_ident(name: str) -> str:
    """Safely quote an identifier (table/column). Does not support multi-part identifiers here.
    We will split db.table if needed.
    """
    if name is None:
        return "NULL"
    name = str(name)
    # if the name contains dots (db.table), quote each part
    parts = name.split(".")
    return ".".join(f"`{p}`" for p in parts)


def quote_db_table(db: str, table: str) -> str:
    return f"{quote_ident(db)}.{quote_ident(table)}"


def get_connection(cfg: Dict):
    mysql_cfg = cfg["mysql"].copy()
    if not mysql_cfg.get("password"):
        mysql_cfg["password"] = getpass.getpass("MySQL password: ")
    conn = mysql.connector.connect(
        host=mysql_cfg.get("host", "localhost"),
        port=mysql_cfg.get("port", 3306),
        user=mysql_cfg.get("user", "root"),
        password=mysql_cfg.get("password", ""),
        autocommit=False,
    )
    return conn


def list_abd_tables(conn, abd_db: str, pattern: str) -> List[str]:
    """Return list of table names in abd_db matching regex pattern"""
    cur = conn.cursor()
    sql = (
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME REGEXP %s"
    )
    cur.execute(sql, (abd_db, pattern))
    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]


def get_table_columns(conn, db: str, table: str) -> List[str]:
    cur = conn.cursor()
    sql = (
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
    )
    cur.execute(sql, (db, table))
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


def build_abd_union_query(conn, cfg: Dict) -> str:
    """Builds a UNION ALL query string that selects from each ABD monthly table and adds abd_month.
    For missing columns, we return NULL AS column.
    """
    abd_db = cfg["databases"]["abd"]
    pattern = cfg.get("abd_table_pattern")
    mapping = cfg["columns"]["abd"]

    tables = list_abd_tables(conn, abd_db, pattern)
    if not tables:
        raise RuntimeError(f"No ABD tables found in {abd_db} matching pattern {pattern}")

    selects = []
    for t in tables:
        cols = set(get_table_columns(conn, abd_db, t))
        # mandatory join keys in abd
        if mapping["emplid"] not in cols or mapping["project_id"] not in cols:
            logger.warning("Skipping table %s because it doesn't have required keys (%s, %s)", t, mapping["emplid"], mapping["project_id"])
            continue

        parts = []
        # emplid and project_id must exist (we enforced above)
        parts.append(f"`{mapping['emplid']}` AS emplid")
        parts.append(f"`{mapping['project_id']}` AS project_id")
        # other columns - if present select them else NULL
        for logical_col in [
            "job_code_description",
            "band",
            "technicalbsgsalessupport",
            "project_type_desc",
            "project_pricing_type",
        ]:
            actual = mapping.get(logical_col)
            if actual and actual in cols:
                parts.append(f"`{actual}` AS {logical_col}")
            else:
                parts.append(f"NULL AS {logical_col}")

        # abd_month: use the table name itself as month label (e.g. '01_2025')
        safe_table_literal = t.replace("'", "\\'")
        parts.append(f"'{safe_table_literal}' AS abd_month")

        select_sql = f"SELECT {', '.join(parts)} FROM {quote_db_table(abd_db, t)}"
        selects.append(select_sql)
    if not selects:
        raise RuntimeError("No usable ABD tables found after checking columns.")

    union_sql = "\nUNION ALL\n".join(selects)
    return union_sql


def create_temp_abd_combined(conn, cfg: Dict, dry_run=False) -> None:
    cur = conn.cursor()
    union_sql = build_abd_union_query(conn, cfg)
    tmp_table = "abd_combined"
    drop_sql = f"DROP TEMPORARY TABLE IF EXISTS `{tmp_table}`;"
    create_sql = f"CREATE TEMPORARY TABLE `{tmp_table}` AS ( {union_sql} );"

    logger.info("Creating temporary ABD combined table with %d source tables (may take some time).", union_sql.count("SELECT"))
    if dry_run:
        logger.info("DRY RUN: would execute DROP and CREATE TEMPORARY TABLE statements.")
        logger.info(drop_sql)
        logger.info(create_sql[:1000] + "\n... (truncated)")
        cur.close()
        return

    cur.execute(drop_sql)
    cur.execute(create_sql)
    conn.commit()
    cur.close()


def create_consolidated_table(conn, cfg: Dict, rebuild: bool = False) -> None:
    db = cfg["databases"]["uk"]
    tbl = cfg["tables"]["consolidated"]
    full = quote_db_table(db, tbl)
    cur = conn.cursor()

    # DDL for consolidated table - adjust types if you want
    ddl = f"""
CREATE TABLE IF NOT EXISTS {full} (
  `EMPLID` VARCHAR(64),
  `Month` VARCHAR(16),
  `GROSS_PAY` DOUBLE,
  `JOB_CODE_DESCRIPTION` TEXT,
  `BAND` VARCHAR(128),
  `technicalbsgsalessupport` TEXT,
  `CURRENT_WORK_LOCATION` TEXT,
  `PROJECT_ID` VARCHAR(128),
  `PROJECT_DESCRIPTION` TEXT,
  `project_type_desc` TEXT,
  `project_pricing_type` TEXT,
  `contract_type` TEXT,
  `cust_name` TEXT,
  `PGM_MANAGER_NAME` VARCHAR(255),
  `PGM_MANAGER_EMAIL` VARCHAR(255)
) ENGINE=InnoDB;
"""
    if rebuild:
        drop = f"DROP TABLE IF EXISTS {full};"
        logger.info("Rebuilding consolidated table %s", full)
        cur.execute(drop)
        cur.execute(ddl)
        # primary key if configured
        pk = cfg.get("consolidated_options", {}).get("primary_key")
        if pk:
            pk_cols = ", ".join([f"`{c}`" for c in pk])
            try:
                cur.execute(f"ALTER TABLE {full} ADD PRIMARY KEY ({pk_cols});")
            except Exception as e:
                logger.warning("Could not add primary key: %s", e)
        conn.commit()
    else:
        logger.info("Ensuring consolidated table exists: %s", full)
        cur.execute(ddl)
        conn.commit()
    cur.close()


def build_final_insert_sql(cfg: Dict) -> str:
    uk_db = cfg["databases"]["uk"]
    abd_tmp = "abd_combined"
    pmr_db = cfg["databases"]["pmr"]

    # column names from mapping
    rmap = cfg["columns"]["regional"]
    smap = cfg["columns"]["salary"]
    amap = cfg["columns"]["abd"]
    pmap = cfg["columns"]["pmr"]

    regional = quote_db_table(uk_db, cfg["tables"]["regional"])
    salary = quote_db_table(uk_db, cfg["tables"]["salary"])
    pmr = quote_db_table(pmr_db, cfg["tables"]["pmr_managers"])
    consolidated_full = quote_db_table(uk_db, cfg["tables"]["consolidated"])

    # Build expressions with DATE_FORMAT to produce MM_YYYY from dates in regional and salary
    # We qualify columns with table aliases (r, s) to avoid ambiguity in JOINs.
    regional_month_expr = f"DATE_FORMAT(r.`{rmap['utilization_end_dt']}`, '%m_%Y')"
    salary_month_expr = f"DATE_FORMAT(s.`{smap['month']}`, '%m_%Y')"

    select = f"""
SELECT
  r.`{rmap['emplid']}` AS EMPLID,
  {regional_month_expr} AS `Month`,
  s.`{smap['gross_pay']}` AS GROSS_PAY,
  a.job_code_description AS JOB_CODE_DESCRIPTION,
  a.band AS BAND,
  a.technicalbsgsalessupport AS technicalbsgsalessupport,
  r.`{rmap.get('current_work_location')}` AS CURRENT_WORK_LOCATION,
  r.`{rmap['project_id']}` AS PROJECT_ID,
  r.`{rmap.get('project_description')}` AS PROJECT_DESCRIPTION,
  a.project_type_desc AS project_type_desc,
  a.project_pricing_type AS project_pricing_type,
  r.`{rmap.get('contract_type')}` AS contract_type,
  r.`{rmap.get('cust_name')}` AS cust_name,
  pm.`{pmap.get('manager_name')}` AS PGM_MANAGER_NAME,
  pm.`{pmap.get('manager_email')}` AS PGM_MANAGER_EMAIL
FROM {regional} r
LEFT JOIN {salary} s
  ON s.`{smap['emplid']}` = r.`{rmap['emplid']}`
  AND {salary_month_expr} = {regional_month_expr}
LEFT JOIN `{abd_tmp}` a
  ON a.emplid = r.`{rmap['emplid']}`
  AND a.project_id = r.`{rmap['project_id']}`
  AND a.abd_month = {regional_month_expr}
LEFT JOIN {pmr} pm
  ON pm.`{pmap['project_id']}` = r.`{rmap['project_id']}`
"""

    # final insert - support configurable insert modes (insert / ignore / replace / on_duplicate)
    cols = ['`EMPLID`','`Month`','`GROSS_PAY`','`JOB_CODE_DESCRIPTION`','`BAND`','`technicalbsgsalessupport`','`CURRENT_WORK_LOCATION`','`PROJECT_ID`','`PROJECT_DESCRIPTION`','`project_type_desc`','`project_pricing_type`','`contract_type`','`cust_name`','`PGM_MANAGER_NAME`','`PGM_MANAGER_EMAIL`']
    insert_mode = cfg.get('behavior', {}).get('insert_mode', 'insert').lower()

    if insert_mode == 'insert':
        verb = 'INSERT INTO'
        suffix = ''
    elif insert_mode == 'ignore':
        verb = 'INSERT IGNORE INTO'
        suffix = ''
    elif insert_mode == 'replace':
        verb = 'REPLACE INTO'
        suffix = ''
    elif insert_mode in ('on_duplicate', 'onduplicate', 'duplicate'):
        verb = 'INSERT INTO'
        # build ON DUPLICATE KEY UPDATE clause updating all columns except primary key(s)
        pk = [p.upper() for p in cfg.get('consolidated_options', {}).get('primary_key', [])]
        plain_cols = [c.strip('`') for c in cols]
        update_assignments = []
        for c in plain_cols:
            if c.upper() in pk:
                continue
            update_assignments.append(f"`{c}` = VALUES(`{c}`)")
        ondup = ' ON DUPLICATE KEY UPDATE ' + ', '.join(update_assignments) if update_assignments else ''
        suffix = ondup
    else:
        verb = 'INSERT INTO'
        suffix = ''

    insert_sql = f"{verb} {consolidated_full} ({', '.join(cols)})
{select}{suffix};"
    return insert_sql


def run_consolidation(cfg: Dict, rebuild: bool = False, dry_run: bool = False) -> None:
    conn = get_connection(cfg)
    try:
        # 1. Create temporary abd_combined
        create_temp_abd_combined(conn, cfg, dry_run=dry_run)

        # 2. Ensure consolidated table
        create_consolidated_table(conn, cfg, rebuild=rebuild)

        # 3. Build insert SQL
        insert_sql = build_final_insert_sql(cfg)

        if dry_run:
            logger.info("DRY RUN - Final INSERT SQL (truncated):\n%s\n...", insert_sql[:2000])
            return

        logger.info("Starting insertion into consolidated table (this may take time).")
        cur = conn.cursor()
        # Execute the insert
        cur.execute(insert_sql)
        conn.commit()
        logger.info("Inserted %d rows into consolidated.", cur.rowcount if hasattr(cur, 'rowcount') else -1)
        cur.close()

    finally:
        conn.close()


# ---------------------------
# CLI
# ---------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consolidate UK regional/salary/ABD/PMR into uk.consolidated")
    parser.add_argument('--rebuild', action='store_true', help='Drop and recreate consolidated table before inserting')
    parser.add_argument('--dry-run', action='store_true', help='Only print SQLs and exit')
    parser.add_argument('--prompt-pass', action='store_true', help='Prompt for MySQL password instead of using config')

    args = parser.parse_args()

    if args.prompt_pass:
        CONFIG['mysql']['password'] = None

    try:
        run_consolidation(CONFIG, rebuild=args.rebuild, dry_run=args.dry_run)
        if args.dry_run:
            logger.info("Dry run complete. No changes were made.")
        else:
            logger.info("Consolidation finished successfully.")
    except Exception as e:
        logger.exception("An error occurred during consolidation: %s", e)
        sys.exit(2)
