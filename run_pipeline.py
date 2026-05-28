import sys
import logging
from src.utils import get_connection
from src.audit import open_batch, close_batch, write_rejected_records
from src import ingest, validate, transform

# --- CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    conn = get_connection()
    conn.autocommit = False
    batch_id = None

    try:
        batch_id = open_batch(conn, process_name="olist_pipeline", source="olist_csv")
        logger.info(f"Pipeline started: batch_id={batch_id}")

        stg_rows_inserted = ingest.main(conn)
        errors = validate.main(conn, batch_id)
        write_rejected_records(conn, batch_id, errors)
        wh_rows_inserted, wh_rows_updated = transform.main(conn)

        close_batch(
            conn,
            batch_id,
            status="success",
            rows_inserted=wh_rows_inserted,
            rows_updated=wh_rows_updated,
            rows_rejected=len(errors),
        )
        logger.info(f"Pipeline complete: batch_id={batch_id}")

    except Exception as e:
        logger.exception(f"Pipeline failed: batch_id={batch_id}")
        conn.rollback()
        if batch_id:
            close_batch(conn, batch_id, status="failure", error_message=str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
