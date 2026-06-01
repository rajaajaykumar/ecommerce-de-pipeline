import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from src.ingest import load_csv, truncate_table, bulk_insert, main

DATA_DIR = Path(__file__).parent / "data"
VALID_CSV = DATA_DIR / "orders_valid.csv"
EMPTY_CSV = DATA_DIR / "orders_empty.csv"
MISSING_COL_CSV = DATA_DIR / "orders_missing_col.csv"
NULLS_CSV = DATA_DIR / "orders_nulls.csv"

ORDERS_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


# ==================================================
# load_csv: no DB needed, pure file I/O
# ==================================================


def test_load_csv_valid():
    """Return DataFrame with correct columns and row count."""
    df = load_csv(VALID_CSV, ORDERS_COLUMNS)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ORDERS_COLUMNS
    assert len(df) == 3


def test_load_csv_missing_file():
    """Raise FileNotFoundError when file does not exist."""
    with pytest.raises(FileNotFoundError):
        load_csv(DATA_DIR / "nonexistent.csv", ORDERS_COLUMNS)


def test_load_csv_missing_columns():
    """Raise ValueError when a required column is absent from the CSV."""
    with pytest.raises(ValueError, match="missing columns"):
        load_csv(MISSING_COL_CSV, ORDERS_COLUMNS)


def test_load_csv_returns_only_required_columns():
    """DataFrame contains exactly the required columns."""
    df = load_csv(VALID_CSV, ORDERS_COLUMNS)
    assert set(df.columns) == set(ORDERS_COLUMNS)


def test_load_csv_all_dtypes_are_str():
    """All columns are read as strings regardless of content."""
    df = load_csv(VALID_CSV, ORDERS_COLUMNS)
    non_string = [
        col
        for col in df.columns
        if df[col].dtype != object and str(df[col].dtype) != "str"
    ]
    assert non_string == [], f"Non-string columns: {non_string}"


def test_load_csv_preserves_all_rows_with_nulls():
    """Load all rows - null cells do not cause row drops."""
    df = load_csv(NULLS_CSV, ORDERS_COLUMNS)
    assert len(df) == 3


# ==================================================
# truncate_table: uses real test DB via conftest
# ==================================================


def test_truncate_table(test_conn, seed_staging):
    """Table is empty after truncate"""
    seed_staging("staging.stg_orders", str(VALID_CSV))

    with test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM staging.stg_orders;")
        assert cur.fetchone()[0] == 3

    truncate_table(test_conn, "staging.stg_orders")

    with test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM staging.stg_orders;")
        assert cur.fetchone()[0] == 0


# ==================================================
# bulk_insert: uses test DB via conftest
# ==================================================


def test_bulk_insert_returns_correct_row_count(test_conn):
    """bulk_insert returns the number of rows in the DataFrame."""
    df = load_csv(VALID_CSV, ORDERS_COLUMNS)
    count = bulk_insert(
        test_conn, "staging.stg_orders", ORDERS_COLUMNS, batch_id=1, df=df
    )
    assert count == 3


def test_bulk_insert_batch_id_populated_on_all_rows(test_conn):
    """Every inserted row carries the correct batch_id."""
    df = load_csv(VALID_CSV, ORDERS_COLUMNS)
    bulk_insert(test_conn, "staging.stg_orders", ORDERS_COLUMNS, batch_id=92, df=df)

    with test_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM staging.stg_orders WHERE batch_id = 92;")
        assert cur.fetchone()[0] == 3


def test_bulk_insert_empty_strings_stored_as_null(test_conn):
    """Empty string cells in the CSV are stored as NULL in the DB."""
    df = load_csv(NULLS_CSV, ORDERS_COLUMNS)
    bulk_insert(test_conn, "staging.stg_orders", ORDERS_COLUMNS, batch_id=1, df=df)

    with test_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM staging.stg_orders
            WHERE customer_id IS NULL
               OR order_id IS NULL
               OR order_status IS NULL;
        """)
        # assert cur.fetchone()[0] > 0
        assert cur.fetchone()[0] == 3


# ==================================================
# main: patches file I/O, uses test DB via conftest
# ==================================================


def test_main_returns_total_inserted(test_conn):
    """main() returns total rows inserted across all manifest tables."""

    dummy_df = pd.DataFrame({"dummy": [1]})

    with patch("src.ingest.load_csv", return_value=dummy_df), patch(
        "src.ingest.bulk_insert", return_value=3
    ) as mock_insert:

        total = main(test_conn, batch_id=1)

    assert total == 12
    assert mock_insert.call_count == 4


def test_main_raises_on_empty_dataframe(test_conn):
    """main() raises ValueError when load_csv returns an empty DataFrame."""
    empty_df = pd.DataFrame(columns=ORDERS_COLUMNS)

    with patch("src.ingest.load_csv", return_value=empty_df):
        with pytest.raises(ValueError, match="contains no data"):
            main(test_conn, batch_id=1)


def test_main_calls_truncate_before_insert(test_conn):
    """Truncate runs before insert for each table — order matters for idempotency."""
    call_order = []
    dummy_df = load_csv(VALID_CSV, ORDERS_COLUMNS)

    def mock_truncate(conn, table):
        call_order.append(("truncate", table))

    def mock_insert(conn, table, columns, batch_id, df):
        call_order.append(("insert", table))
        return len(df)

    with patch("src.ingest.load_csv", return_value=dummy_df), patch(
        "src.ingest.truncate_table", side_effect=mock_truncate
    ), patch("src.ingest.bulk_insert", side_effect=mock_insert):
        main(test_conn, batch_id=1)

    for i in range(0, len(call_order), 2):
        assert call_order[i][0] == "truncate"
        assert call_order[i + 1][0] == "insert"
        assert call_order[i][1] == call_order[i + 1][1]
