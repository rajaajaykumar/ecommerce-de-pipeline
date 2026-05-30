import sys
import logging
from datetime import date
from src.utils import get_connection

# --- CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(message)s",
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
    logger.info("%s row count: %s", table, count)


def check_nulls(cur, table: str, required_cols: list[str]) -> list:
    """
    Validate that required columns contain no NULL values.
    """
    nulls = []

    for col in required_cols:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL;")
        null_count = cur.fetchone()[0]
        if null_count > 0:
            nulls.append(
                {
                    "table": table,
                    "pk_value": None,
                    "reason": f"NULL in required column: {col} ({null_count} rows affected)",
                    "affected_count": null_count,
                }
            )

    logger.info(f"Null check completed ({len(required_cols)} columns checked)")
    return nulls


def check_duplicates(cur, table: str, primary_key: list[str]) -> list:
    """
    Validate that specified primary key columns are unique across all rows.
    """
    dupes = []
    pk_cols = ", ".join(primary_key)
    cur.execute(
        f"SELECT {pk_cols}, COUNT(*) AS cnt FROM {table} GROUP BY {pk_cols} HAVING COUNT(*) > 1 ORDER BY cnt DESC;"
    )

    duplicates = cur.fetchall()
    if duplicates:
        for row in duplicates:
            dupes.append(
                {
                    "table": table,
                    "pk_value": str(row[:-1]),
                    "reason": f"DUPLICATE PK: {pk_cols} (count: {row[-1]})",
                    "affected_count": row[-1],
                }
            )

    logger.info(f"Duplicate check completed (PK: {pk_cols}).")
    return dupes


def main(conn) -> list[dict]:
    logger.info("Starting validation")
    all_errors = []
    total_errors = 0

    with conn.cursor() as cur:
        for entry in VALIDATE_MANIFEST:
            logger.info(f"Validating {entry['table']}")
            check_row_count(cur, entry["table"])
            errors = []
            errors.extend(check_nulls(cur, entry["table"], entry["required_columns"]))
            errors.extend(check_duplicates(cur, entry["table"], entry["primary_key"]))

            if errors:
                total_errors += len(errors)
                all_errors.extend(errors)
                logger.warning(f"{entry['table']}: {len(errors)} issues quarantined")
            else:
                logger.info(f"{entry['table']} passed all checks")

    if total_errors == 0:
        logger.info("Validation complete - all tables passed")
    else:
        logger.warning(
            f"Validation complete - {total_errors} issues across all tables quarantined"
        )

    return all_errors


if __name__ == "__main__":
    conn = None

    try:
        conn = get_connection()
        conn.autocommit = False
        main(conn)
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Execution failed")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
