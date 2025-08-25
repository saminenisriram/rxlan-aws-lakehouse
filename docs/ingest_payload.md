
### D) `docs/ingest_payload.md`
```md
# Ingest Payload Contract (Step 1)

Each city produces one JSON object printed as a single line.

## Required fields
- `app` (string) – e.g., "rxlan"
- `stage` (string) – e.g., "dev"
- `source` (string) – "openweather"
- `fetched_at_utc` (ISO8601, Z)
- `city` (string)
- `country` (string, 2-letter)
- `lat` (float), `lon` (float)
- `temp_c` (float)
- `feels_like_c` (float)
- `humidity` (int, 0–100)
- `pressure` (int, >0)
- `wind_speed` (float)

## Optional fields
- `clouds_pct` (int)
- `weather_main` (string)
- `weather_desc` (string)
- `raw` (object) – small subset of original API for debugging

## Example (shape only)
```json
{
  "app": "rxlan",
  "stage": "dev",
  "source": "openweather",
  "fetched_at_utc": "2025-08-25T16:10:03Z",
  "city": "Austin",
  "country": "US",
  "lat": 30.27,
  "lon": -97.74,
  "temp_c": 33.1,
  "feels_like_c": 35.0,
  "humidity": 52,
  "pressure": 1011,
  "wind_speed": 3.2,
  "clouds_pct": 40,
  "weather_main": "Clouds",
  "weather_desc": "scattered clouds",
  "raw": { "id": "...", "dt": 1692970203 }
}
