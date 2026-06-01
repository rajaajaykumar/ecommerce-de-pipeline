import pytest
from src.audit import open_batch, close_batch, write_rejected_records

# ==================================================
# open_batch
# ==================================================


def test_open_batch_returns_positive_integer(test_conn):
    """open_batch returns a positive integer batch_id."""
    batch_id = open_batch(test_conn, process_name="test", source="test_csv")
    assert isinstance(batch_id, int)
    assert batch_id > 0


def test_open_batch_status_is_running(test_conn):
    """Audit row has status='running' immediately after open_batch."""
    batch_id = open_batch(test_conn, process_name="test", source="test_csv")
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM audit.etl_audit_batch WHERE batch_id = %s",
            (batch_id,),
        )
        status = cur.fetchone()[0]
    assert status == "running"


def test_open_batch_end_time_is_null(test_conn):
    """end_time is NULL on an open batch — not yet closed."""
    batch_id = open_batch(test_conn, process_name="test", source="test_csv")
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT end_time FROM audit.etl_audit_batch WHERE batch_id = %s",
            (batch_id,),
        )
        end_time = cur.fetchone()[0]
    assert end_time is None


def test_open_batch_each_call_returns_unique_id(test_conn):
    """Each open_batch call produces a distinct batch_id."""
    id1 = open_batch(test_conn, process_name="test", source="test_csv")
    id2 = open_batch(test_conn, process_name="test", source="test_csv")
    assert id1 != id2


# ==================================================
# close_batch
# ==================================================


def test_close_batch_sets_success_status(test_conn):
    """close_batch updates status to 'success'."""
    batch_id = open_batch(test_conn, process_name="test", source="test_csv")
    close_batch(test_conn, batch_id, status="success")
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM audit.etl_audit_batch WHERE batch_id = %s",
            (batch_id,),
        )
        assert cur.fetchone()[0] == "success"


def test_close_batch_sets_failure_status(test_conn):
    """close_batch updates status to 'failure'."""
    batch_id = open_batch(test_conn, process_name="test", source="test_csv")
    close_batch(test_conn, batch_id, status="failure", error_message="something broke")
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM audit.etl_audit_batch WHERE batch_id = %s",
            (batch_id,),
        )
        assert cur.fetchone()[0] == "failure"


def test_close_batch_populates_end_time(test_conn):
    """end_time is set after close_batch."""
    batch_id = open_batch(test_conn, process_name="test", source="test_csv")
    close_batch(test_conn, batch_id, status="success")
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT end_time FROM audit.etl_audit_batch WHERE batch_id = %s",
            (batch_id,),
        )
        assert cur.fetchone()[0] is not None


def test_close_batch_stores_error_message(test_conn):
    """error_message is persisted on failure."""
    batch_id = open_batch(test_conn, process_name="test", source="test_csv")
    close_batch(
        test_conn, batch_id, status="failure", error_message="db connection lost"
    )
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT error_message FROM audit.etl_audit_batch WHERE batch_id = %s",
            (batch_id,),
        )
        assert cur.fetchone()[0] == "db connection lost"


def test_close_batch_stores_row_counts(test_conn):
    """rows_inserted, rows_updated, rows_rejected are all persisted correctly."""
    batch_id = open_batch(test_conn, process_name="test", source="test_csv")
    close_batch(
        test_conn,
        batch_id,
        status="success",
        rows_inserted=100,
        rows_updated=5,
        rows_rejected=3,
    )
    with test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT rows_inserted, rows_updated, rows_rejected
            FROM audit.etl_audit_batch
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        inserted, updated, rejected = cur.fetchone()
    assert inserted == 100
    assert updated == 5
    assert rejected == 3


def test_close_batch_error_message_null_on_success(test_conn):
    """error_message remains NULL on successful close when not provided."""
    batch_id = open_batch(test_conn, process_name="test", source="test_csv")
    close_batch(test_conn, batch_id, status="success")
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT error_message FROM audit.etl_audit_batch WHERE batch_id = %s",
            (batch_id,),
        )
        assert cur.fetchone()[0] is None


# ==================================================
# write_rejected_records
# ==================================================


def test_write_rejected_records_inserts_correct_count(test_conn, open_test_batch):
    """Correct number of rows written to audit.rejected_records."""
    errors = [
        {
            "table": "staging.stg_orders",
            "reason": "NULL in order_id",
            "affected_count": 2,
        },
        {"table": "staging.stg_orders", "reason": "DUPLICATE PK", "affected_count": 1},
    ]
    write_rejected_records(test_conn, open_test_batch, errors)
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM audit.rejected_records WHERE batch_id = %s",
            (open_test_batch,),
        )
        assert cur.fetchone()[0] == 2


def test_write_rejected_records_batch_id_matches(test_conn, open_test_batch):
    """All written records reference the correct batch_id."""
    errors = [
        {
            "table": "staging.stg_customers",
            "reason": "NULL in customer_id",
            "affected_count": 1,
        },
    ]
    write_rejected_records(test_conn, open_test_batch, errors)
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT batch_id FROM audit.rejected_records WHERE batch_id = %s",
            (open_test_batch,),
        )
        assert cur.fetchone()[0] == open_test_batch


def test_write_rejected_records_empty_list_inserts_nothing(test_conn, open_test_batch):
    """Empty errors list writes nothing and does not raise."""
    write_rejected_records(test_conn, open_test_batch, [])
    with test_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM audit.rejected_records WHERE batch_id = %s",
            (open_test_batch,),
        )
        assert cur.fetchone()[0] == 0


def test_write_rejected_records_stores_reason_and_count(test_conn, open_test_batch):
    """reason and affected_count are persisted correctly per record."""
    errors = [
        {
            "table": "staging.stg_products",
            "reason": "NULL in product_id",
            "affected_count": 4,
        },
    ]
    write_rejected_records(test_conn, open_test_batch, errors)
    with test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT reason, affected_count
            FROM audit.rejected_records
            WHERE batch_id = %s
            """,
            (open_test_batch,),
        )
        reason, count = cur.fetchone()
    assert reason == "NULL in product_id"
    assert count == 4
