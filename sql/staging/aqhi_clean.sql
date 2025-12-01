-- Cleans raw AQHI data and adds risk category

DROP TABLE IF EXISTS staging.aqhi_clean;

CREATE TABLE staging.aqhi_clean AS
SELECT
    datetime_utc,
    station_code,
    aqhi_value,
    CASE
        WHEN aqhi_value IS NULL THEN NULL
        WHEN aqhi_value <= 3 THEN 'Low'
        WHEN aqhi_value <= 6 THEN 'Moderate'
        WHEN aqhi_value <= 10 THEN 'High'
        ELSE 'Very High'
    END AS risk_category,
    source_file
FROM raw.aqhi_long
WHERE aqhi_value IS NOT NULL        -- remove null AQHI values
  AND aqhi_value >= 0              -- remove impossible negative values
  AND aqhi_value <= 20;            -- sanity cap, AQHI normally 1-10+
