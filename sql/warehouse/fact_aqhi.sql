-- 1) Create table
CREATE TABLE IF NOT EXISTS warehouse.fact_aqhi (
    aqhi_fact_key      BIGSERIAL PRIMARY KEY,
    station_key        INTEGER NOT NULL,
    date_time_key      INTEGER NOT NULL,
    aqhi_value         NUMERIC(4,1) NOT NULL,
    risk_category      TEXT NOT NULL,
    source_file        TEXT,
    load_timestamp     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2) Clear for rebuild
TRUNCATE TABLE warehouse.fact_aqhi;

-- 3) Load from staging layer and join to dimensions
INSERT INTO warehouse.fact_aqhi (
    station_key,
    date_time_key,
    aqhi_value,
    risk_category,
    source_file
)
SELECT
    ds.station_key,
    dd.date_time_key,
    c.aqhi_value,
    c.risk_category,
    c.source_file
FROM staging.aqhi_clean AS c
JOIN warehouse.dim_station AS ds
    ON ds.station_code = c.station_code
JOIN warehouse.dim_date AS dd
    ON dd.datetime_utc = date_trunc('hour', c.datetime_utc);
