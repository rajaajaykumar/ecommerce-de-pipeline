import sys
import logging
from pathlib import Path
from src.utils import get_connection

logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "transform.sql"


def main(conn) -> tuple[int, int]:
    logger.info("Running SQL transformations")
    with open(SQL_PATH, "r") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    logger.info("Transformations complete")
    # TODO: Replace placeholder counts with actual warehouse insert/update metrics during SCD2
    return 0, 0


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
