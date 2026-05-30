import sys
import logging
from pathlib import Path
from src.utils import get_connection

logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "transform.sql"


def reconcile(conn, rows_inserted: int) -> None:
    """
    Compares staging vs warehouse counts.
    Logs a reconciliation summary and a warning if no fact rows inserted.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM staging.stg_order_items) AS stg_items,
                (SELECT COUNT(*) FROM warehouse.fact_orders) AS fact_totals,
                (SELECT COUNT(*) FROM warehouse.dim_customers WHERE is_current = TRUE) AS current_customers,
                (SELECT COUNT(*) FROM warehouse.dim_products WHERE is_current = TRUE) AS current_products,
                (SELECT COUNT(*) FROM warehouse.dim_customers WHERE is_current = FALSE) AS expired_customers,
                (SELECT COUNT(*) FROM warehouse.dim_products WHERE is_current = FALSE) AS expired_products
        """)
        r = cur.fetchone()

    (
        stage_items,
        fact_total,
        cur_customers,
        cur_products,
        exp_customers,
        exp_products,
    ) = r

    logger.info("--- Reconciliation Summary ---")
    logger.info(f"Staging order items : {stage_items:,}")
    logger.info(f"Fact rows this run : {rows_inserted:,}")
    logger.info(f"Fact rows total : {fact_total:,}")
    logger.info(f"dim_customers current : {cur_customers:,} (expired: {exp_customers})")
    logger.info(f"dim_products current : {cur_products:,} (expired: {exp_products})")
    logger.info("------------------------------")

    if rows_inserted == 0:
        logger.warning(
            "Reconciliation: 0 fact rows inserted - possible duplicate run or empty staging"
        )


def main(conn) -> tuple[int, int]:
    logger.info("Running SQL transformations")
    with open(SQL_PATH, "r") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(sql)
        rows_inserted = cur.rowcount  # fact_orders must remain last in transform.sql
    logger.info(f"Transformations complete: {rows_inserted} fact rows inserted")

    # TODO: rows_updated
    reconcile(conn, rows_inserted)
    return rows_inserted, 0


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
