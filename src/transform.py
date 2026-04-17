import os
import logging
from utils import get_connection


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SQL_PATH = os.path.join(os.path.dirname(__file__), "transform.sql")


def main() -> None:
    logger.info("Running SQL transformations")

    with open(SQL_PATH, "r") as f:
        sql = f.read()
    conn = get_connection()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        logger.info("Transformations committed")
    except Exception as e:
        conn.rollback()
        logger.error(f"Transform failed - rolling back: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
