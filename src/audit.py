import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def open_batch(conn, process_name: str, source: str) -> int:
    """
    Creates a new batch entry with status='running' and returns the generated batch_id.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.etl_audit_batch (process_name, source, status, start_time)
            VALUES (%s, %s, 'running', NOW())
            RETURNING batch_id;
            """,
            (process_name, source),
        )
        batch_id = cur.fetchone()[0]
    conn.commit()
    logger.info(f"Batch opened: batch_id={batch_id}")
    return batch_id


def close_batch(
    conn,
    batch_id: int,
    status: str,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_rejected: int = 0,
    error_message: str = None,
) -> None:
    """
    Closes the batch with final status and counts.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE audit.etl_audit_batch
            SET status         = %s,
                end_time       = NOW(),
                rows_inserted  = %s,
                rows_updated   = %s,
                rows_rejected  = %s,
                error_message  = %s
            WHERE batch_id = %s;
            """,
            (
                status,
                rows_inserted,
                rows_updated,
                rows_rejected,
                error_message,
                batch_id,
            ),
        )
    conn.commit()
    logger.info(f"Batch closed: batch_id={batch_id} status={status}")


def write_rejected_records(conn, batch_id: int, errors: list[dict]) -> None:
    """
    Writes quarantined validation errors to audit.rejected_records.
    """
    if not errors:
        return

    with conn.cursor() as cur:
        for error in errors:
            cur.execute(
                """
                INSERT INTO audit.rejected_records
                    (batch_id, source_table, reason, affected_count)
                VALUES (%s, %s, %s, %s);
                """,
                (
                    batch_id,
                    error.get("table"),
                    error.get("reason"),
                    error.get("affected_count", 1),
                ),
            )
    conn.commit()
    logger.info(f"Wrote {len(errors)} rejected records for batch_id={batch_id}")
