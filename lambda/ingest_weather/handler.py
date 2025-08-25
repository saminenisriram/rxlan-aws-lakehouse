"""
rxlan-ingest-weather (Step 1)

What this Lambda will do (later when we add code):
- Read env vars: OW_API_KEY, OW_BASE_URL, CITY_LIST, UNITS, APP_NAME, STAGE
- Call OpenWeather for each city
- Normalize payload (app, stage, source, fetched_at_utc, city, country, lat, lon, temp_c, feels_like_c, humidity, pressure, wind_speed, raw)
- Basic validation (ranges, required fields)
- print() one JSON line per city to CloudWatch Logs
- Return summary: {"cities": N, "ok": X, "errors": Y}

TODO (you):
- Fill in the actual code after we confirm Step 1 scaffolding.
"""