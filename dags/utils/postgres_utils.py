import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def load_to_raw_upsert(df, table: str, dag_run_id: str, conflict_columns: list) -> int:
    hook = PostgresHook(postgres_conn_id="commodity_postgres")

    df["_ingested_at"] = datetime.utcnow()
    df["_dag_run_id"] = dag_run_id

    conn = hook.get_conn()
    cursor = conn.cursor()

    columns = list(df.columns)
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)

    update_cols = [c for c in columns if c not in conflict_columns]
    update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    conflict_clause = ", ".join(conflict_columns)

    sql = f"""
        INSERT INTO {table} ({col_names})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {update_clause}
    """

    rows = [tuple(row) for row in df.itertuples(index=False)]
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()

    logger.info(f"Upserted {len(rows)} rows into {table}")
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