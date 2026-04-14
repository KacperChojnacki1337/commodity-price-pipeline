CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.eia_prices (
    id              SERIAL PRIMARY KEY,
    price_date      DATE         NOT NULL,
    commodity       VARCHAR(50)  NOT NULL,
    price_usd       NUMERIC(10,4),
    unit            VARCHAR(30),
    series_id       VARCHAR(50),
    _ingested_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source         VARCHAR(20)  NOT NULL DEFAULT 'eia',
    _dag_run_id     VARCHAR(200),
    CONSTRAINT uq_eia_prices_date_commodity UNIQUE (price_date, commodity)
);

CREATE TABLE IF NOT EXISTS raw.yfinance_prices (
    id              SERIAL PRIMARY KEY,
    price_date      DATE         NOT NULL,
    ticker          VARCHAR(20)  NOT NULL,
    open            NUMERIC(10,4),
    high            NUMERIC(10,4),
    low             NUMERIC(10,4),
    close           NUMERIC(10,4),
    volume          BIGINT,
    _ingested_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    _source         VARCHAR(20)  NOT NULL DEFAULT 'yfinance',
    _dag_run_id     VARCHAR(200),
    CONSTRAINT uq_yfinance_prices_date_ticker UNIQUE (price_date, ticker)
);

CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(200),
    task_id         VARCHAR(200),
    source          VARCHAR(20),
    execution_date  DATE,
    rows_loaded     INT,
    status          VARCHAR(20),
    error_message   TEXT,
    duration_sec    NUMERIC(8,2),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_eia_prices_date        ON raw.eia_prices(price_date);
CREATE INDEX IF NOT EXISTS idx_eia_prices_commodity   ON raw.eia_prices(commodity);
CREATE INDEX IF NOT EXISTS idx_yfinance_prices_date   ON raw.yfinance_prices(price_date);
CREATE INDEX IF NOT EXISTS idx_yfinance_prices_ticker ON raw.yfinance_prices(ticker);