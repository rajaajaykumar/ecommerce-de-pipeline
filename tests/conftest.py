import os
import pytest
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SCHEMA_SQL_PATH = Path(__file__).parent.parent / "db/schema.sql"

STAGING_TABLES = [
    "staging.stg_orders",
    "staging.stg_order_items",
    "staging.stg_customers",
    "staging.stg_products",
]

WAREHOUSE_TABLES = [
    "warehouse.fact_orders",
    "warehouse.dim_customers",
    "warehouse.dim_products",
    "warehouse.dim_time",
]

AUDIT_TABLES = [
    "audit.rejected_records",
    "audit.etl_audit_batch",
]


# --- CONNECTION ---
def get_test_connection():
    """
    Return a psycopg2 connection to the test database.
    """
    return psycopg2.connect(
        dbname=os.getenv("TEST_POSTGRES_DB"),
        user=os.getenv("TEST_POSTGRES_USER"),
        password=os.getenv("TEST_POSTGRES_PASSWORD"),
        host=os.getenv("TEST_POSTGRES_HOST", "localhost"),
        port=os.getenv("TEST_POSTGRES_PORT", "5432"),
    )


# --- SESSION-SCOPED: run schema DDL once per test session ---
@pytest.fixture(scope="session")
def setup_schema():
    """
    Run schema.sql against the test database once per test session.
    Drop and recreate all tables - start from a clean slate.
    """
    conn = get_test_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        with open(SCHEMA_SQL_PATH, "r") as f:
            cur.execute(f.read())
    conn.close()


# --- FUNCTION-SCOPED: connection per test, rollback after ---
@pytest.fixture
def test_conn(setup_schema):
    """
    Yield a psycopg2 connection for a single test.
    Rollback after test completion - no data persists between tests.
    """
    conn = get_test_connection()
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


# --- HELPER: truncate all tables between tests if needed ---
@pytest.fixture
def clean_tables(test_conn):
    """
    Truncate all staging, warehouse, and audit tables.
    Use in tests that need a fully empty DB state.
    Cascade to handle FK constraints.
    """
    with test_conn.cursor() as cur:
        for table in AUDIT_TABLES + WAREHOUSE_TABLES + STAGING_TABLES:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;")
    yield


# --- HELPER: seed a single staging table from a CSV file ---
@pytest.fixture
def seed_staging(test_conn):
    """
    Return a callable that loads a CSV into a staging table.
    Usage in tests:
        seed_staging("staging.stg_customers", "tests/data/customers_valid.csv")
    Infer columns from the CSV header row.
    """
    import pandas as pd
    from psycopg2.extras import execute_values

    def _seed(table: str, csv_path: str, batch_id: int = 1) -> int:
        df = pd.read_csv(csv_path, dtype=str).fillna("")

        columns = list(df.columns) + ["batch_id"]
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"

        records = [
            tuple(None if v == "" else v for v in row) + (batch_id,)
            for row in df.itertuples(index=False, name=None)
        ]

        with test_conn.cursor() as cur:
            execute_values(cur, sql, records)

        return len(records)

    return _seed


# --- HELPER: open a batch for tests that need a batch_id ---
@pytest.fixture
def open_test_batch(test_conn):
    """
    Open a batch in audit.etl_audit_batch and return the batch_id.
    Useful for tests that need a valid FK reference in audit.rejected_records.
    """
    with test_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audit.etl_audit_batch (process_name, source, status, start_time)
            VALUES ('test_pipeline', 'test', 'running', NOW())
            RETURNING batch_id;
            """)
        batch_id = cur.fetchone()[0]
    return batch_id
