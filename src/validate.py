import os
import logging
import psycopg2
from utils import get_connection


# --- CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

VALIDATE_MANIFEST = [
    {
        "table": "staging.stg_orders",
        "required_columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
        ],
        "primary_key": ["order_id"],
    },
    {
        "table": "staging.stg_order_items",
        "required_columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "price",
        ],
        "primary_key": ["order_id", "order_item_id"],
    },
    {
        "table": "staging.stg_customers",
        "required_columns": [
            "customer_id",
            "customer_unique_id",
        ],
        "primary_key": ["customer_id"],
    },
    {
        "table": "staging.stg_products",
        "required_columns": ["product_id"],
        "primary_key": ["product_id"],
    },
]


def check_row_count(cur, table: str) -> None:
    """
    Validate that the table contains at least one row. Fail if table is empty.
    """
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError(f"{table} is empty")
    logger.info(f"Row count: {count}")


def check_nulls(cur, table: str, required_cols: list[str]) -> None:
    """
    Validate that required columns contain no NULL values.
    """
    nulls = []

    for col in required_cols:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL;")
        null_count = cur.fetchone()[0]
        if null_count > 0:
            nulls.append(f"{col}: {null_count:,} null rows")

    if nulls:
        raise ValueError(f"{table} null check failed:\n" + "\n".join(nulls))
    logger.info(f"  Null check passed ({len(required_cols)} columns checked)")


def check_duplicates(cur, table: str, primary_key: list[str]) -> None:
    """
    Validate that specified primary key columns are unique across all rows.
    """
    pk_cols = ", ".join(primary_key)
    cur.execute(
        f"SELECT {pk_cols}, COUNT(*) AS cnt FROM {table} GROUP BY {pk_cols} HAVING COUNT(*) > 1 ORDER BY cnt DESC;"
    )

    duplicates = cur.fetchall()
    if duplicates:
        raise ValueError(
            f"{table} duplicate found in primary keys:\n    {"\n    ".join(str(row) for row in duplicates)}"
        )
    logger.info(f"Duplicate check passed (PK: {pk_cols}).")


def main() -> None:
    logger.info("=" * 10 + " Starting validation " + "=" * 10)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for entry in VALIDATE_MANIFEST:
                logger.info(f"Validating {entry["table"]}")
                check_row_count(cur, entry["table"])
                check_nulls(cur, entry["table"], entry["required_columns"])
                check_duplicates(cur, entry["table"], entry["primary_key"])
                logger.info(f"{entry["table"]} passed all checks")
        logger.info("=" * 10 + " Validation complete - all tables passed " + "=" * 10)
    except ValueError as e:
        logger.error("=" * 10 + f" Validation failed:\n{e} " + "=" * 10)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    # try:
    main()
    # except Exception:
    #     sys.exit(1)
