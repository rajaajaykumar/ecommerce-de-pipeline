from airflow import DAG
from pendulum import datetime, duration
from airflow.operators.python import PythonOperator

from src.utils import get_connection
from src.audit import open_batch, close_batch, write_rejected_records
from src import ingest, validate, transform


def task_open_batch():
    conn = get_connection()
    try:
        batch_id = open_batch(conn, process_name="olist_pipeline", source="olist_csv")
        # open_batch commits internally
        return {"batch_id": batch_id}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def task_ingest(ti):
    batch_id = ti.xcom_pull(task_ids="open_batch")["batch_id"]
    conn = get_connection()
    try:
        ingest.main(conn, batch_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def task_validate(ti):
    batch_id = ti.xcom_pull(task_ids="open_batch")["batch_id"]
    conn = get_connection()
    try:
        errors = validate.main(conn)
        write_rejected_records(conn, batch_id, errors)
        conn.commit()
        return {"rows_rejected": len(errors)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def task_transform(ti):
    batch_id = ti.xcom_pull(task_ids="open_batch")["batch_id"]
    conn = get_connection()
    try:
        wh_rows_inserted, wh_rows_updated = transform.main(conn, batch_id)
        conn.commit()
        return {"rows_inserted": wh_rows_inserted, "rows_updated": wh_rows_updated}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def task_close_batch(ti):
    batch_id = ti.xcom_pull(task_ids="open_batch")["batch_id"]
    rows_rejected = ti.xcom_pull(task_ids="validate")["rows_rejected"]
    transform_result = ti.xcom_pull(task_ids="transform")
    rows_inserted, rows_updated = (
        transform_result["rows_inserted"],
        transform_result["rows_updated"],
    )

    conn = get_connection()
    try:
        close_batch(
            conn,
            batch_id,
            status="success",
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_rejected=rows_rejected,
        )
        # close_batch commits internally
    finally:
        conn.close()


def on_dag_failure(**context):
    ti = context["ti"]

    open_result = ti.xcom_pull(task_ids="open_batch")
    batch_id = open_result["batch_id"] if open_result else None

    if not batch_id:
        return

    validate_result = ti.xcom_pull(task_ids="validate") or {}
    transform_result = ti.xcom_pull(task_ids="transform") or {}

    rows_rejected = validate_result.get("rows_rejected", 0)
    rows_inserted = transform_result.get("rows_inserted", 0)
    rows_updated = transform_result.get("rows_updated", 0)

    conn = get_connection()
    try:
        close_batch(
            conn,
            batch_id,
            status="failure",
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_rejected=rows_rejected,
            error_message=str(context.get("exception")),
        )
        # close_batch commits internally
    finally:
        conn.close()


default_args = {
    "owner": "jarvis",
    "retries": 1,
    "retry_delay": duration(minutes=5),
}

with DAG(
    dag_id="olist_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    on_failure_callback=on_dag_failure,
) as dag:

    t_open = PythonOperator(task_id="open_batch", python_callable=task_open_batch)
    t_ingest = PythonOperator(task_id="ingest", python_callable=task_ingest)
    t_validate = PythonOperator(task_id="validate", python_callable=task_validate)
    t_transform = PythonOperator(task_id="transform", python_callable=task_transform)
    t_close = PythonOperator(task_id="close_batch", python_callable=task_close_batch)

    t_open >> t_ingest >> t_validate >> t_transform >> t_close
