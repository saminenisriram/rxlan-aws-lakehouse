CREATE TABLE IF NOT EXISTS public.weather (
  app             VARCHAR(32),
  stage           VARCHAR(16),
  source          VARCHAR(32),
  fetched_at_utc  VARCHAR(32),         
  city            VARCHAR(80),
  country         VARCHAR(3),
  lat             DOUBLE PRECISION,
  lon             DOUBLE PRECISION,
  temp_c          DOUBLE PRECISION,
  feels_like_c    DOUBLE PRECISION,
  humidity        INTEGER,
  pressure        INTEGER,
  wind_speed      DOUBLE PRECISION
)
DISTSTYLE AUTO
SORTKEY (city, fetched_at_utc);

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'weather';

SELECT COUNT(*) AS rows FROM public.weather;

SELECT city, country, temp_c, fetched_at_utc, ts, dt, hour, loaded_at
FROM public.weather
ORDER BY loaded_at DESC
LIMIT 20;

SELECT COUNT(*) AS fully_null_payload_rows
FROM public.weather
WHERE app IS NULL AND stage IS NULL AND source IS NULL
  AND fetched_at_utc IS NULL AND city IS NULL AND country IS NULL
  AND lat IS NULL AND lon IS NULL AND temp_c IS NULL
  AND feels_like_c IS NULL AND humidity IS NULL AND pressure IS NULL
  AND wind_speed IS NULL;


-- Null distribution for a few key columns
SELECT
  SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS null_city,
  SUM(CASE WHEN temp_c IS NULL THEN 1 ELSE 0 END) AS null_temp,
  SUM(CASE WHEN ts IS NULL THEN 1 ELSE 0 END) AS null_ts
FROM public.weather;

-- Potential duplicates by (city, ts)
SELECT city, ts, COUNT(*) AS c
FROM public.weather
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY c DESC, city
LIMIT 20;

SELECT *
FROM public.weather