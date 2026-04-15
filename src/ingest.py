import os
import sys
import logging
import pandas as pd
from psycopg2.extras import execute_values
from utils import get_connection


# --- CONFIG ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")
logger = logging.getLogger(__name__)

INPUT_DIR = "data/raw/"
REQUIRED_METADATA = [
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
def load_csv(filepath: str, required_cols: list[str]) -> pd.DataFrame:
    """
    Reads a CSV file, converts the whole dataset to str, and keeps only required columns. Returns a DataFrame.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    logger.info(f"Reading {filepath}")

    df = pd.read_csv(filepath, dtype=str)

    actual_cols = df.columns
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
def bulk_insert(conn, table: str, columns: list[str], df: pd.DataFrame) -> int:
    """
    Inserts DataFrame rows into table using execute_values for efficient batch inserts. Returns the number of rows inserted.
    """
    sql = f"INSERT INTO {table} ({", ".join(columns)}) VALUES %s"

    records = []
    for row in df.itertuples(index=False, name=None):
        record = tuple(None if pd.isna(v) else v for v in row)
        records.append(record)

    with conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=1000)

    return len(records)


# --- MAIN ---
def main() -> None:
    """
    Orchestrates the ingestion process from CSV to Database.
    """
    logger.info("Starting ingestion")
    conn = get_connection()
    conn.autocommit = False

    try:
        for entry in REQUIRED_METADATA:
            logger.info(f"Ingesting {entry['table']}")

            filepath = INPUT_DIR + entry["csv_file"]
            df = load_csv(filepath, entry["columns"])
            truncate_table(conn, entry["table"])
            rows_inserted = bulk_insert(conn, entry["table"], entry["columns"], df)
            logger.info(f"Inserted {rows_inserted} rows into {entry["table"]}")

        conn.commit()
        logger.info("Ingestion complete - all tables commited")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ingestion failed - rolling back: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
