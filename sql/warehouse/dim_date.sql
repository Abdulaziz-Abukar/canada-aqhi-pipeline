WITH bounds AS (
	SELECT
		MIN(datetime_utc) AS min_dt,
		MAX(datetime_utc) AS max_dt
	FROM raw.aqhi_long
),
series AS (
	SELECT
		generate_series(
			date_trunc('hour', min_dt),
			date_trunc('hour', max_dt),
			interval '1 hour'
		) AS datetime_utc
	FROM bounds
)
INSERT INTO warehouse.dim_date (
	date_time_key,
	datetime_utc,
	date,
	year,
	quarter,
	month,
	month_name,
	day,
	day_of_week,
	day_name,
	week_of_year,
	hour,
	is_weekend
)
SELECT
	-- YYYYMMDDHH as an integer
	(EXTRACT(YEAR 	FROM s.datetime_utc)::INT * 1000000) +
	(EXTRACT(MONTH 	FROM s.datetime_utc)::INT * 10000) +
	(EXTRACT(DAY	FROM s.datetime_utc)::INT * 100) +
	(EXTRACT(HOUR 	FROM s.datetime_utc)::INT)							AS date_time_key,
	s.datetime_utc														AS datetime_utc,
	(s.datetime_utc AT TIME ZONE 'UTC')::DATE							AS date,
	EXTRACT(YEAR	FROM s.datetime_utc AT TIME ZONE 'UTC')::INT		AS year,
	EXTRACT(QUARTER FROM s.datetime_utc AT TIME ZONE 'UTC')::INT		AS quarter,
	EXTRACT(MONTH	FROM s.datetime_utc AT TIME ZONE 'UTC')::INT		AS month,
	TO_CHAR(s.datetime_utc AT TIME ZONE 'UTC', 'Month')					AS month_name,
	EXTRACT(DAY		FROM s.datetime_utc AT TIME ZONE 'UTC')::INT		AS day,
	EXTRACT(DOW		FROM s.datetime_utc AT TIME ZONE 'UTC')::INT + 1	AS day_of_week,		-- 1-7
	TO_CHAR(s.datetime_utc AT TIME ZONE 'UTC', 'Day')					AS day_name,
	EXTRACT(WEEK 	FROM s.datetime_utc AT TIME ZONE 'UTC')::INT		AS week_of_year,
	EXTRACT(HOUR	FROM s.datetime_utc AT TIME ZONE 'UTC')::INT		AS hour,
	CASE
		WHEN EXTRACT(DOW FROM s.datetime_utc AT TIME ZONE 'UTC') IN (5, 6)
			THEN TRUE	-- Friday(5) or Saturday (6) as weekend
		ELSE FALSE
	END AS is_weekend
FROM series s
ORDER BY s.datetime_utc;
