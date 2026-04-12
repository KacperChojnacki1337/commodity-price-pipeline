import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from utils.postgres_utils import load_to_raw, log_ingestion

logger = logging.getLogger(__name__)

EIA_BASE_URL = "https://api.eia.gov/v2"

# Commodities które pobieramy:
# seria EIA → (nazwa tabeli, jednostka, opis)
COMMODITIES = {
    "PET.RWTC.D": ("crude_oil_wti", "dollars_per_barrel"),
    "NG.RNGWHHD.D": ("natural_gas_henry_hub", "dollars_per_mmbtu"),
}

default_args = {
    "owner": "kacper",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="ingest_eia_commodity_prices",
    description="Pobiera dzienne ceny surowców z EIA API i zapisuje do raw.eia_prices",
    schedule="0 8 * * 1-5",  # każdy dzień roboczy o 8:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingest", "eia", "raw"],
)
def ingest_eia_dag():

    @task()
    def fetch_eia_prices(**context) -> dict:
        """
        Pobiera ceny z EIA API dla wszystkich zdefiniowanych serii.
        Zwraca słownik z listą rekordów — trafi do XCom automatycznie.
        """
        api_key = os.environ.get("EIA_API_KEY")
        if not api_key:
            raise ValueError("EIA_API_KEY nie jest ustawiony w .env")

        execution_date = context["ds"]  # format YYYY-MM-DD
        # Pobieramy ostatnie 7 dni żeby złapać weekendy i opóźnienia
        start_date = (datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

        all_records = []

        for series_id, (commodity, unit) in COMMODITIES.items():
            logger.info(f"Fetching {series_id} ({commodity})")

            url = f"{EIA_BASE_URL}/seriesid/{series_id}"
            params = {
                "api_key": api_key,
                "data[0]": "value",
                "start": start_date,
                "end": execution_date,
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 10,
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            records = data.get("response", {}).get("data", [])

            if not records:
                logger.warning(f"Brak danych dla {series_id}")
                continue

            for rec in records:
                all_records.append({
                    "price_date": rec["period"],
                    "commodity": commodity,
                    "price_usd": rec.get("value"),
                    "unit": unit,
                    "series_id": series_id,
                    "_source": "eia",
                })

            logger.info(f"Pobrano {len(records)} rekordów dla {commodity}")

        logger.info(f"Łącznie pobrano {len(all_records)} rekordów")
        return {"records": all_records, "execution_date": execution_date}

    @task()
    def load_eia_to_raw(payload: dict, **context) -> int:
        """
        Zapisuje pobrane dane do raw.eia_prices.
        Zwraca liczbę załadowanych wierszy — trafi do XCom.
        """
        start_time = time.time()
        records = payload["records"]
        execution_date = payload["execution_date"]

        if not records:
            logger.warning("Brak rekordów do załadowania")
            log_ingestion(
                dag_id=context["dag"].dag_id,
                task_id=context["task"].task_id,
                source="eia",
                execution_date=execution_date,
                rows_loaded=0,
                status="no_data",
            )
            return 0

        df = pd.DataFrame(records)

        # Usuń duplikaty — ten sam dzień + commodity
        df = df.drop_duplicates(subset=["price_date", "commodity"])

        rows_loaded = load_to_raw(
            df=df,
            table="raw.eia_prices",
            dag_run_id=context["run_id"],
        )

        duration = round(time.time() - start_time, 2)

        log_ingestion(
            dag_id=context["dag"].dag_id,
            task_id=context["task"].task_id,
            source="eia",
            execution_date=execution_date,
            rows_loaded=rows_loaded,
            status="success",
            duration_sec=duration,
        )

        return rows_loaded

    @task()
    def verify_load(rows_loaded: int, **context) -> None:
        """
        Sprawdza czy dane faktycznie trafiły do bazy.
        Prosty sanity check po każdym ingeście.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        execution_date = context["ds"]

        hook = PostgresHook(postgres_conn_id="commodity_postgres")
        count = hook.get_first(
            """
            SELECT COUNT(*)
            FROM raw.eia_prices
            WHERE price_date >= %s::date - interval '7 days'
            """,
            parameters=(execution_date,),
        )[0]

        logger.info(f"Wierszy w raw.eia_prices z ostatnich 7 dni: {count}")
        logger.info(f"Załadowano w tym runie: {rows_loaded}")

        if rows_loaded > 0 and count == 0:
            raise ValueError("Dane nie trafiły do bazy mimo pozytywnego zapisu!")

    # Definicja przepływu
    payload = fetch_eia_prices()
    rows = load_eia_to_raw(payload)
    verify_load(rows)


ingest_eia_dag()