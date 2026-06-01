import pytest
from pathlib import Path
from src.validate import check_row_count, check_nulls, check_duplicates, main

DATA_DIR = Path(__file__).parent / "data"
CUST_VALID_CSV = DATA_DIR / "customers_valid.csv"
CUST_NULLS_CSV = DATA_DIR / "customers_nulls.csv"
CUST_DUPLICATES_CSV = DATA_DIR / "customers_duplicates.csv"
ORDERS_VALID_CSV = DATA_DIR / "orders_valid.csv"
ORDERS_NULLS_CSV = DATA_DIR / "orders_nulls.csv"
ORDERS_DUPLICATES_CSV = DATA_DIR / "orders_duplicates.csv"

TABLE = "staging.stg_customers"
REQUIRED = ["customer_id", "customer_unique_id"]
PK = ["customer_id"]


# ==================================================
# check_row_count
# ==================================================


def test_check_row_count_passes(test_conn, seed_staging):
    """No exception raised when table has rows."""
    seed_staging(TABLE, str(CUST_VALID_CSV))
    with test_conn.cursor() as cur:
        check_row_count(cur, TABLE)  # should not raise


def test_check_row_count_raises_on_empty_table(test_conn, clean_tables):
    """Raise ValueError when table is empty."""
    with test_conn.cursor() as cur:
        with pytest.raises(ValueError, match="is empty"):
            check_row_count(cur, TABLE)


# ==================================================
# check_nulls
# ==================================================


def test_check_nulls_returns_empty_on_clean_data(test_conn, seed_staging):
    """Return empty list when no NULLs exist in required columns."""
    seed_staging(TABLE, str(CUST_VALID_CSV))
    with test_conn.cursor() as cur:
        errors = check_nulls(cur, TABLE, REQUIRED)
    assert errors == []


def test_check_nulls_returns_one_error_per_affected_column(test_conn, seed_staging):
    """Return one error dict per column that contains NULLs."""
    seed_staging(TABLE, str(CUST_NULLS_CSV))
    with test_conn.cursor() as cur:
        errors = check_nulls(cur, TABLE, REQUIRED)
    # customers_nulls.csv has NULLs in both customer_id and customer_unique_id
    assert len(errors) == 2


def test_check_nulls_error_dict_has_required_keys(test_conn, seed_staging):
    """Each error dict contains table, pk_value, reason, affected_count."""
    seed_staging(TABLE, str(CUST_NULLS_CSV))
    with test_conn.cursor() as cur:
        errors = check_nulls(cur, TABLE, REQUIRED)
    for error in errors:
        assert "table" in error
        assert "pk_value" in error
        assert "reason" in error
        assert "affected_count" in error


def test_check_nulls_affected_count_matches_actual_nulls(test_conn, seed_staging):
    """affected_count in error dict matches the actual null row count in DB."""
    seed_staging(TABLE, str(CUST_NULLS_CSV))
    with test_conn.cursor() as cur:
        errors = check_nulls(cur, TABLE, ["customer_id"])
        # customers_nulls.csv has exactly 1 null customer_id
        assert errors[0]["affected_count"] == 1


def test_check_nulls_pk_value_is_none(test_conn, seed_staging):
    """pk_value is None for null checks - row cannot be identified by a null PK."""
    seed_staging(TABLE, str(CUST_NULLS_CSV))
    with test_conn.cursor() as cur:
        errors = check_nulls(cur, TABLE, REQUIRED)
    assert all(e["pk_value"] is None for e in errors)


# ==================================================
# check_duplicates
# ==================================================


def test_check_duplicates_returns_empty_on_unique_data(test_conn, seed_staging):
    """Return empty list when all PK values are unique."""
    seed_staging(TABLE, str(CUST_VALID_CSV))
    with test_conn.cursor() as cur:
        errors = check_duplicates(cur, TABLE, PK)
    assert errors == []


def test_check_duplicates_returns_errors_on_dupes(test_conn, seed_staging):
    """Return one error dict per duplicate PK group."""
    seed_staging(TABLE, str(CUST_DUPLICATES_CSV))
    with test_conn.cursor() as cur:
        errors = check_duplicates(cur, TABLE, PK)
    assert len(errors) == 1


def test_check_duplicates_pk_value_identifies_duplicate(test_conn, seed_staging):
    """pk_value in error dict contains the actual duplicate key value."""
    seed_staging(TABLE, str(CUST_DUPLICATES_CSV))
    with test_conn.cursor() as cur:
        errors = check_duplicates(cur, TABLE, PK)
    assert "cust_001" in errors[0]["pk_value"]


def test_check_duplicates_affected_count_is_occurrence_count(test_conn, seed_staging):
    """affected_count reflects how many times the PK appears."""
    seed_staging(TABLE, str(CUST_DUPLICATES_CSV))
    with test_conn.cursor() as cur:
        errors = check_duplicates(cur, TABLE, PK)
    # cust_001 appears twice in customers_duplicates.csv
    assert errors[0]["affected_count"] == 2


def test_check_duplicates_error_dict_has_required_keys(test_conn, seed_staging):
    """Each error dict contains table, pk_value, reason, affected_count."""
    seed_staging(TABLE, str(CUST_DUPLICATES_CSV))
    with test_conn.cursor() as cur:
        errors = check_duplicates(cur, TABLE, PK)
    for error in errors:
        assert "table" in error
        assert "pk_value" in error
        assert "reason" in error
        assert "affected_count" in error


# ==================================================
# main
# ==================================================


def test_main_returns_empty_list_on_clean_data(test_conn, seed_staging):
    """main() returns empty list when all tables pass validation."""
    seed_staging("staging.stg_orders", str(ORDERS_VALID_CSV))
    seed_staging("staging.stg_customers", str(CUST_VALID_CSV))
    with test_conn.cursor() as cur:
        # seed remaining tables with minimal valid data to pass row count check
        cur.execute("""
            INSERT INTO staging.stg_order_items
                (order_id, order_item_id, product_id, price, batch_id)
            VALUES ('order_001', '1', 'prod_001', '99.90', 1);
        """)
        cur.execute("""
            INSERT INTO staging.stg_products (product_id, batch_id)
            VALUES ('prod_001', 1);
        """)
    errors = main(test_conn)
    assert errors == []


def test_main_returns_errors_without_raising(test_conn, seed_staging):
    """main() returns error list on soft failures - does not raise."""
    seed_staging("staging.stg_orders", str(ORDERS_NULLS_CSV))
    seed_staging("staging.stg_customers", str(CUST_NULLS_CSV))
    with test_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO staging.stg_order_items
                (order_id, order_item_id, product_id, price, batch_id)
            VALUES ('order_001', '1', 'prod_001', '99.90', 1);
        """)
        cur.execute("""
            INSERT INTO staging.stg_products (product_id, batch_id)
            VALUES ('prod_001', 1);
        """)
    errors = main(test_conn)
    assert isinstance(errors, list)
    assert len(errors) > 0


def test_main_collects_errors_across_all_tables(test_conn, seed_staging):
    """main() aggregates errors from every table in the manifest."""
    seed_staging("staging.stg_orders", str(ORDERS_NULLS_CSV))
    seed_staging("staging.stg_customers", str(CUST_NULLS_CSV))
    with test_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO staging.stg_order_items
                (order_id, order_item_id, product_id, price, batch_id)
            VALUES ('order_001', '1', 'prod_001', '99.90', 1);
        """)
        cur.execute("""
            INSERT INTO staging.stg_products (product_id, batch_id)
            VALUES ('prod_001', 1);
        """)
    errors = main(test_conn)
    tables_with_errors = {e["table"] for e in errors}
    assert "staging.stg_orders" in tables_with_errors
    assert "staging.stg_customers" in tables_with_errors


def test_main_raises_on_empty_table(test_conn, clean_tables):
    """main() raises ValueError when any staging table is empty."""
    with pytest.raises(ValueError, match="is empty"):
        main(test_conn)
