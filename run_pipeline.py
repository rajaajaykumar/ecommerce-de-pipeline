import sys
import time
import logging
from src import ingest, validate, transform


# --- CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- RUN (STAGE) ---
def run_stage(name: str, func) -> float:
    """
    Runs pipeline stage and returns elapsed time in seconds.
    """
    logger.info("=" * 20 + f" STAGE: {name.upper()} " + "=" * 20)
    start_time = time.time()
    try:
        func()
    except Exception as e:
        logger.error(f"Stage '{name}' failed: {e}")
        raise
    elapsed = time.time() - start_time
    logger.info(f"STAGE '{name}' completed in {elapsed:.2f}s")
    return elapsed


def main() -> None:
    start_time = time.time()
    stages = [
        ("ingest", ingest.main),
        ("validate", validate.main),
        ("transform", transform.main),
    ]

    for name, func in stages:
        run_stage(name, func)

    total = time.time() - start_time
    logger.info("=" * 15 + f" Pipeline completed in {total:.2f}s " + "=" * 15)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
