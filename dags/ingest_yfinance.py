import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

import yfinance as yf

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.postgres_utils import load_to_raw_upsert, log_ingestion

logger = logging.getLogger(__name__)

TICKERS = {
    "USO":  "crude_oil_etf",
    "BNO":  "crude_oil_brent_etf",
    "UNG":  "natural_gas_etf",
    "CPER": "copper_etf",
}

default_args = {
    "owner": "kacper",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def get_session() -> requests.Session:
    """Tworzy sesję HTTP z nagłówkami przeglądarki — Yahoo blokuje requesty bez nich."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return session


@dag(
    dag_id="ingest_yfinance_commodity_prices",
    description="Pobiera dzienne ceny ETF surowców z Yahoo Finance i zapisuje do raw.yfinance_prices",
    schedule="0 9 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingest", "yfinance", "raw"],
)
def ingest_yfinance_dag():

    @task()
    def fetch_yfinance_prices(**context) -> dict:
        execution_date = context["ds"]
        start_date = (
            datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=7)
        ).strftime("%Y-%m-%d")

        session = get_session()
        all_records = []

        for ticker, commodity_name in TICKERS.items():
            logger.info(f"Fetching {ticker} ({commodity_name})")

            try:
                tk = yf.Ticker(ticker, session=session)
                data = tk.history(start=start_date, end=execution_date)

                if data.empty:
                    logger.warning(f"Brak danych dla {ticker}")
                    continue

                data = data.reset_index()
                data.columns = [c.lower().replace(" ", "_") for c in data.columns]

                for _, row in data.iterrows():
                    all_records.append({
                        "price_date": row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"])[:10],
                        "ticker":  ticker,
                        "open":    round(float(row["open"]), 4)   if pd.notna(row.get("open"))   else None,
                        "high":    round(float(row["high"]), 4)   if pd.notna(row.get("high"))   else None,
                        "low":     round(float(row["low"]), 4)    if pd.notna(row.get("low"))    else None,
                        "close":   round(float(row["close"]), 4)  if pd.notna(row.get("close"))  else None,
                        "volume":  int(row["volume"])             if pd.notna(row.get("volume")) else None,
                        "_source": "yfinance",
                    })

                logger.info(f"Pobrano {len(data)} rekordów dla {ticker}")

            except Exception as e:
                logger.error(f"Błąd przy pobieraniu {ticker}: {e}")
                continue

        logger.info(f"Łącznie pobrano {len(all_records)} rekordów")
        return {"records": all_records, "execution_date": execution_date}

    @task()
    def load_yfinance_to_raw(payload: dict, **context) -> int:
        start_time = time.time()
        records = payload["records"]
        execution_date = payload["execution_date"]

        if not records:
            logger.warning("Brak rekordów do załadowania")
            log_ingestion(
                dag_id=context["dag"].dag_id,
                task_id=context["task"].task_id,
                source="yfinance",
                execution_date=execution_date,
                rows_loaded=0,
                status="no_data",
            )
            return 0

        df = pd.DataFrame(records)
        df = df.drop_duplicates(subset=["price_date", "ticker"])

        rows_loaded = load_to_raw_upsert(
            df=df,
            table="raw.yfinance_prices",
            dag_run_id=context["run_id"],
            conflict_columns=["price_date", "ticker"],
        )

        duration = round(time.time() - start_time, 2)

        log_ingestion(
            dag_id=context["dag"].dag_id,
            task_id=context["task"].task_id,
            source="yfinance",
            execution_date=execution_date,
            rows_loaded=rows_loaded,
            status="success",
            duration_sec=duration,
        )

        return rows_loaded

    @task()
    def verify_load(rows_loaded: int, **context) -> None:
        execution_date = context["ds"]

        hook = PostgresHook(postgres_conn_id="commodity_postgres")
        count = hook.get_first(
            """
            SELECT COUNT(*)
            FROM raw.yfinance_prices
            WHERE price_date >= %s::date - interval '7 days'
            """,
            parameters=(execution_date,),
        )[0]

        logger.info(f"Wierszy w raw.yfinance_prices z ostatnich 7 dni: {count}")
        logger.info(f"Załadowano w tym runie: {rows_loaded}")

        if rows_loaded > 0 and count == 0:
            raise ValueError("Dane nie trafiły do bazy mimo pozytywnego zapisu!")

    payload = fetch_yfinance_prices()
    rows = load_yfinance_to_raw(payload)
    verify_load(rows)


ingest_yfinance_dag()