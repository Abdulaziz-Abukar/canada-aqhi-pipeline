-- Summary table with basic station metadata from observational data

DROP TABLE IF EXISTS staging.station_usage;

CREATE TABLE staging.station_usage AS
SELECT
    station_code,
    MIN(datetime_utc) AS first_observation_utc,
    MAX(datetime_utc) AS last_observation_utc,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT DATE(datetime_utc)) AS active_days
FROM staging.aqhi_clean
GROUP BY station_code;
