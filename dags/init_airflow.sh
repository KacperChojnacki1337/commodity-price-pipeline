#!/bin/bash
set -e

airflow db migrate

airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

airflow connections add 'commodity_postgres' \
  --conn-type postgres \
  --conn-host postgres-data \
  --conn-login ${COMMODITY_DB_USER} \
  --conn-password ${COMMODITY_DB_PASSWORD} \
  --conn-schema ${COMMODITY_DB_NAME} \
  --conn-port 5432 || true

echo "Airflow init done."
