import sys
import logging
from pathlib import Path
from src.utils import get_connection

logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "transform.sql"


def get_expired_counts(conn) -> tuple[int, int]:
    """
    Return counts of expired SCD2 records for dim_customers and dim_products.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM warehouse.dim_customers WHERE is_current = FALSE),
                (SELECT COUNT(*) FROM warehouse.dim_products WHERE is_current = FALSE)
        """)
        return cur.fetchone()


def reconcile(conn, rows_inserted: int, rows_updated: int) -> None:
    """
    Log reconciliation metrics for staging, fact, and dimension tables.
    Report rows inserted/updated during the current run and warn if no fact rows are inserted.
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
    logger.info(f"Fact rows inserted this run : {rows_inserted:,}")
    logger.info(f"Fact rows total : {fact_total:,}")
    logger.info(f"dim_customers current : {cur_customers:,} (expired: {exp_customers})")
    logger.info(f"dim_products current : {cur_products:,} (expired: {exp_products})")
    logger.info(f"Dimension rows updated this run : {rows_updated:,}")
    logger.info("------------------------------")

    if rows_inserted == 0:
        logger.warning(
            "Reconciliation: 0 fact rows inserted - possible duplicate run or empty staging"
        )


def main(conn, batch_id: int) -> tuple[int, int]:
    logger.info("Running SQL transformations")

    before_cust, before_prod = get_expired_counts(conn)

    with open(SQL_PATH, "r") as f:
        sql = f.read()

    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL app.batch_id = {str(batch_id)}")
        cur.execute(sql)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM warehouse.fact_orders WHERE batch_id = %s",
            (batch_id,),
        )
        rows_inserted = cur.fetchone()[0]
    logger.info(f"Transformations complete: {rows_inserted} fact rows inserted")

    after_cust, after_prod = get_expired_counts(conn)

    rows_updated = (after_cust - before_cust) + (after_prod - before_prod)

    reconcile(conn, rows_inserted, rows_updated)
    return rows_inserted, rows_updated


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
