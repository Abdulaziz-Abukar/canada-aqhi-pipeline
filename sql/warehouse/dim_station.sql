-- Create table (idempotent)
CREATE TABLE IF NOT EXISTS warehouse.dim_station (
    station_key           SERIAL PRIMARY KEY,
    station_code          TEXT NOT NULL UNIQUE,
    station_name          TEXT,
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    obs_url               TEXT,
    fcst_url              TEXT,
    first_observation_utc TIMESTAMPTZ,
    last_observation_utc  TIMESTAMPTZ,
    observation_count     BIGINT,
    active_days           INTEGER
);

TRUNCATE TABLE warehouse.dim_station;

-- Load data
INSERT INTO warehouse.dim_station (
    station_code,
    station_name,
    latitude,
    longitude,
    obs_url,
    fcst_url,
    first_observation_utc,
    last_observation_utc,
    observation_count,
    active_days
)
SELECT
    s.station_code,
    s.station_name,
    s.latitude,
    s.longitude,
    s.obs_url,
    s.fcst_url,
    u.first_observation_utc,
    u.last_observation_utc,
    u.observation_count,
    u.active_days
FROM raw.aqhi_stations_raw AS s
LEFT JOIN staging.station_usage AS u
    ON s.station_code = u.station_code;
