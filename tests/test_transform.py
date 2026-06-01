import pytest
from pathlib import Path
from src.transform import main as transform_main, reconcile

DATA_DIR = Path(__file__).parent / "data"
ORDERS_VALID = DATA_DIR / "orders_valid.csv"
ORDER_ITEMS_VALID = DATA_DIR / "order_items_valid.csv"
CUSTOMERS_VALID = DATA_DIR / "customers_valid.csv"
CUSTOMERS_CHANGED = DATA_DIR / "customers_changed.csv"
PRODUCTS_CHANGED = DATA_DIR / "products_changed.csv"
PRODUCTS_VALID = DATA_DIR / "products_valid.csv"

BATCH_ID = 1


# ==================================================
# HELPER: seed all four staging tables
# Seeds the minimum required for transform to run end-to-end.
# ==================================================


def seed_all(seed_staging, batch_id=BATCH_ID):
    seed_staging("staging.stg_orders", str(ORDERS_VALID), batch_id)
    seed_staging("staging.stg_order_items", str(ORDER_ITEMS_VALID), batch_id)
    seed_staging("staging.stg_customers", str(CUSTOMERS_VALID), batch_id)
    seed_staging("staging.stg_products", str(PRODUCTS_VALID), batch_id)


# ==================================================
# fact_orders — incremental insert
# ==================================================


def test_fact_orders_inserted_on_first_run(test_conn, clean_tables, seed_staging):
    """fact_orders receives correct row count on first transform run."""
    seed_all(seed_staging)
    rows_inserted, _ = transform_main(test_conn, batch_id=BATCH_ID)
    assert rows_inserted == 2  # order_items_valid.csv has 2 rows


def test_fact_orders_idempotent_on_second_run(test_conn, clean_tables, seed_staging):
    """Second transform run with identical staging data inserts 0 fact rows."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    rows_inserted, _ = transform_main(test_conn, batch_id=BATCH_ID + 1)
    assert rows_inserted == 0


def test_fact_orders_total_count_unchanged_on_second_run(
    test_conn, clean_tables, seed_staging
):
    """Total fact_orders row count does not grow on a duplicate run."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM warehouse.fact_orders;")
        count_after_first = cur.fetchone()[0]

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM warehouse.fact_orders;")
        count_after_second = cur.fetchone()[0]

    assert count_after_first == count_after_second


def test_fact_orders_batch_id_populated(test_conn, clean_tables, seed_staging):
    """Every inserted fact row carries the correct batch_id."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM warehouse.fact_orders WHERE batch_id = %s",
            (BATCH_ID,),
        )
        assert cur.fetchone()[0] == 2


def test_fact_orders_foreign_keys_resolved(test_conn, clean_tables, seed_staging):
    """customer_key, product_key, time_key are all non-NULL after transform."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)
    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM warehouse.fact_orders
            WHERE customer_key IS NULL
               OR product_key  IS NULL
               OR time_key     IS NULL;
        """)
        assert cur.fetchone()[0] == 0


# ==================================================
# dim_time — incremental insert
# ==================================================


def test_dim_time_populated_on_first_run(test_conn, clean_tables, seed_staging):
    """dim_time receives distinct purchase dates from staging.stg_orders."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)
    with test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM warehouse.dim_time;")
        assert cur.fetchone()[0] > 0


def test_dim_time_idempotent_on_second_run(test_conn, clean_tables, seed_staging):
    """Second run does not duplicate dates already in dim_time."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM warehouse.dim_time;")
        count_after_first = cur.fetchone()[0]

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM warehouse.dim_time;")
        count_after_second = cur.fetchone()[0]

    assert count_after_first == count_after_second


def test_dim_time_batch_id_populated(test_conn, clean_tables, seed_staging):
    """dim_time rows carry the batch_id from the run that inserted them."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM warehouse.dim_time WHERE batch_id = %s",
            (BATCH_ID,),
        )
        assert cur.fetchone()[0] > 0


# ==================================================
# transform_main return values
# ==================================================


def test_main_returns_tuple_of_two_ints(test_conn, clean_tables, seed_staging):
    """main() returns a tuple of (rows_inserted, rows_updated)."""
    seed_all(seed_staging)
    result = transform_main(test_conn, batch_id=BATCH_ID)
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert all(isinstance(v, int) for v in result)


# ==================================================
# SESSION 5 — SCD2 dimensions
# ==================================================

# --- dim_customers ---


def test_scd2_new_customer_inserted(test_conn, clean_tables, seed_staging):
    """New customer_id gets exactly one current row in dim_customers."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)
    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM warehouse.dim_customers
            WHERE customer_id = 'cust_001' AND is_current = TRUE;
        """)
        assert cur.fetchone()[0] == 1


def test_scd2_unchanged_customer_no_expiry(test_conn, clean_tables, seed_staging):
    """Re-running with identical customer data does not expire any rows."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)
    transform_main(test_conn, batch_id=BATCH_ID + 1)
    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM warehouse.dim_customers WHERE is_current = FALSE;
        """)
        assert cur.fetchone()[0] == 0


def test_scd2_changed_customer_expires_old_row(test_conn, clean_tables, seed_staging):
    """Changed customer attribute sets is_current=FALSE and effective_to on old row."""
    # Run 1: load original customers
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    # Truncate staging and reload with changed city for cust_001
    with test_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.stg_customers;")
    seed_staging("staging.stg_customers", str(CUSTOMERS_CHANGED), BATCH_ID + 1)

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM warehouse.dim_customers
            WHERE customer_id = 'cust_001'
              AND is_current   = FALSE
              AND effective_to IS NOT NULL;
        """)
        assert cur.fetchone()[0] == 1


def test_scd2_changed_customer_inserts_new_version(
    test_conn, clean_tables, seed_staging
):
    """Changed customer gets a new current row with updated attributes."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with test_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.stg_customers;")
    seed_staging("staging.stg_customers", str(CUSTOMERS_CHANGED), BATCH_ID + 1)

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT city FROM warehouse.dim_customers
            WHERE customer_id = 'cust_001' AND is_current = TRUE;
        """)
        new_city = cur.fetchone()[0]
    # customers_changed.csv has 'curitiba' for cust_001; transform applies INITCAP
    assert new_city == "Curitiba"


def test_scd2_only_one_current_row_per_customer(test_conn, clean_tables, seed_staging):
    """After a change, each customer_id has exactly one is_current=TRUE row."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with test_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.stg_customers;")
    seed_staging("staging.stg_customers", str(CUSTOMERS_CHANGED), BATCH_ID + 1)

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT customer_id, COUNT(*) AS cnt
            FROM warehouse.dim_customers
            WHERE is_current = TRUE
            GROUP BY customer_id
            HAVING COUNT(*) > 1;
        """)
        duplicates = cur.fetchall()
    assert duplicates == []


def test_scd2_customer_new_version_batch_id(test_conn, clean_tables, seed_staging):
    """New SCD2 version carries the batch_id of the run that created it."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with test_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.stg_customers;")
    seed_staging("staging.stg_customers", str(CUSTOMERS_CHANGED), BATCH_ID + 1)

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT batch_id FROM warehouse.dim_customers
            WHERE customer_id = 'cust_001' AND is_current = TRUE;
        """)
        assert cur.fetchone()[0] == BATCH_ID + 1


def test_scd2_expired_row_batch_id_updated(test_conn, clean_tables, seed_staging):
    """Expired row has its batch_id updated to the run that expired it."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with test_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.stg_customers;")
    seed_staging("staging.stg_customers", str(CUSTOMERS_CHANGED), BATCH_ID + 1)

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT batch_id FROM warehouse.dim_customers
            WHERE customer_id = 'cust_001' AND is_current = FALSE;
        """)
        assert cur.fetchone()[0] == BATCH_ID + 1


# --- dim_products ---


def test_scd2_new_product_inserted(test_conn, clean_tables, seed_staging):
    """New product_id gets exactly one current row in dim_products."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)
    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM warehouse.dim_products
            WHERE product_id = 'prod_001' AND is_current = TRUE;
        """)
        assert cur.fetchone()[0] == 1


def test_scd2_changed_product_expires_old_row(test_conn, clean_tables, seed_staging):
    """Changed product attribute expires the old dim_products row."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with test_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.stg_products;")
    seed_staging("staging.stg_products", str(PRODUCTS_CHANGED), BATCH_ID + 1)

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM warehouse.dim_products
            WHERE product_id = 'prod_001'
              AND is_current  = FALSE
              AND effective_to IS NOT NULL;
        """)
        assert cur.fetchone()[0] == 1


def test_scd2_changed_product_inserts_new_version(
    test_conn, clean_tables, seed_staging
):
    """Changed product gets a new current row with updated category."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with test_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.stg_products;")
    seed_staging("staging.stg_products", str(PRODUCTS_CHANGED), BATCH_ID + 1)

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT category_name FROM warehouse.dim_products
            WHERE product_id = 'prod_001' AND is_current = TRUE;
        """)
        new_category = cur.fetchone()[0]
    # products_changed.csv has 'computers'; transform applies INITCAP
    assert new_category == "Computers"


def test_scd2_only_one_current_row_per_product(test_conn, clean_tables, seed_staging):
    """After a change, each product_id has exactly one is_current=TRUE row."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with test_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.stg_products;")
    seed_staging("staging.stg_products", str(PRODUCTS_CHANGED), BATCH_ID + 1)

    transform_main(test_conn, batch_id=BATCH_ID + 1)

    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT product_id, COUNT(*) AS cnt
            FROM warehouse.dim_products
            WHERE is_current = TRUE
            GROUP BY product_id
            HAVING COUNT(*) > 1;
        """)
        duplicates = cur.fetchall()
    assert duplicates == []


# ==================================================
# SESSION 6 — Reconciliation
# ==================================================


def test_reconcile_runs_without_error(test_conn, clean_tables, seed_staging):
    """reconcile() completes without raising on a populated warehouse."""
    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)
    # reconcile is called inside transform_main — reaching here without exception is the assertion
    # call it directly too to confirm it is independently stable
    reconcile(test_conn, rows_inserted=2, rows_updated=0)


def test_reconcile_warns_on_zero_inserts(test_conn, clean_tables, seed_staging, caplog):
    """reconcile() emits a warning when rows_inserted is 0."""
    import logging

    seed_all(seed_staging)
    transform_main(test_conn, batch_id=BATCH_ID)

    with caplog.at_level(logging.WARNING, logger="src.transform"):
        reconcile(test_conn, rows_inserted=0, rows_updated=0)

    assert any("0 fact rows inserted" in record.message for record in caplog.records)
