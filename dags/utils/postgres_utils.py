import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def load_to_raw(df, table: str, dag_run_id: str) -> int:
    hook = PostgresHook(postgres_conn_id="commodity_postgres")

    df["_ingested_at"] = datetime.utcnow()
    df["_dag_run_id"] = dag_run_id

    rows = [tuple(row) for row in df.itertuples(index=False)]
    columns = list(df.columns)

    hook.insert_rows(
        table=table,
        rows=rows,
        target_fields=columns,
        replace=False,
    )

    logger.info(f"Loaded {len(rows)} rows into {table}")
    return len(rows)


def log_ingestion(
    dag_id: str,
    task_id: str,
    source: str,
    execution_date,
    rows_loaded: int,
    status: str,
    error_message: str = None,
    duration_sec: float = None,
) -> None:
    hook = PostgresHook(postgres_conn_id="commodity_postgres")
    hook.run(
        """
        INSERT INTO raw.ingestion_log
            (dag_id, task_id, source, execution_date,
             rows_loaded, status, error_message, duration_sec)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        parameters=(
            dag_id, task_id, source, execution_date,
            rows_loaded, status, error_message, duration_sec,
        ),
    )