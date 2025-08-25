# rxlan-ingest-weather (Step 1)

## Purpose
Fetch current weather for a small city list from OpenWeather, normalize the JSON, and print one line per city to CloudWatch Logs.

## Environment variables (set in Lambda console)
- `OW_API_KEY` (string) – your OpenWeather API key
- `OW_BASE_URL` (string) – e.g. `https://api.openweathermap.org/data/2.5/weather`
- `CITY_LIST` (JSON array string) – e.g. `["Austin,US","Chicago,US"]`
- `UNITS` (string) – `metric` or `imperial`
- `APP_NAME` (string) – `rxlan`
- `STAGE` (string) – `dev`

## Test event (Lambda → Test)
```json
{ "trigger": "manual" }
