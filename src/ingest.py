import os
import sys
import logging
import pandas as pd
from pathlib import Path
from psycopg2.extras import execute_values
from src.utils import get_connection

# --- CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "raw"

INGEST_MANIFEST = [
    {
        "csv_file": "olist_orders_dataset.csv",
        "table": "staging.stg_orders",
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    },
    {
        "csv_file": "olist_order_items_dataset.csv",
        "table": "staging.stg_order_items",
        "columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    },
    {
        "csv_file": "olist_customers_dataset.csv",
        "table": "staging.stg_customers",
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    },
    {
        "csv_file": "olist_products_dataset.csv",
        "table": "staging.stg_products",
        "columns": [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
    },
]


# --- STEP 1: LOAD ---
def load_csv(filepath: Path, required_cols: list[str]) -> pd.DataFrame:
    """
    Read a CSV file, convert the whole dataset to str, and keep only required columns. Return a DataFrame.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    logger.info(f"Reading {filepath}")

    df = pd.read_csv(filepath, dtype=str)

    actual_cols = set(df.columns)
    missing = [c for c in required_cols if c not in actual_cols]
    if missing:
        raise ValueError(f"{filepath} is missing columns: {missing}")

    df = df[required_cols]

    logger.info(f"Rows read: {len(df)}")
    return df


# --- STEP 2: TRUNCATE ---
def truncate_table(conn, table: str) -> None:
    """
    Truncates table.
    """
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table};")
    logger.info(f"Truncated {table}")


# --- STEP 3: INSERT DATA (BATCH) ---
def bulk_insert(
    conn, table: str, columns: list[str], batch_id: int, df: pd.DataFrame
) -> int:
    """
    Insert DataFrame rows into table using execute_values for efficient batch inserts. Return the number of rows inserted.
    """
    cols = ", ".join(columns) + ", batch_id"
    sql = f"INSERT INTO {table} ({cols}) VALUES %s"

    records = []
    for row in df.itertuples(index=False, name=None):
        record = tuple(None if pd.isna(v) or v == "" else v for v in row)
        record += (batch_id,)
        records.append(record)

    with conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=1000)

    return len(records)


# --- MAIN ---
def main(conn, batch_id) -> int:
    """
    Orchestrate the ingestion process from CSV to Database.
    """
    logger.info("Starting ingestion")
    total_inserted = 0
    for entry in INGEST_MANIFEST:
        logger.info(f"Ingesting {entry['table']}")
        filepath = INPUT_DIR / entry["csv_file"]
        df = load_csv(filepath, entry["columns"])
        if df.empty:
            raise ValueError(f"{filepath} contains no data")
        truncate_table(conn, entry["table"])
        rows_inserted = bulk_insert(
            conn, entry["table"], entry["columns"], batch_id, df
        )
        total_inserted += rows_inserted
        logger.info(
            f"Inserted {rows_inserted} rows into {entry['table']} (batch_id={batch_id})"
        )
    logger.info(f"Ingestion complete: total rows inserted={total_inserted}")
    return total_inserted


if __name__ == "__main__":
    conn = None

    try:
        conn = get_connection()
        conn.autocommit = False
        # Placeholder batch_id for standalone testing
        main(conn, batch_id=999999)
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Execution failed")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
