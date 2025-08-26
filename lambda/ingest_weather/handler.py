
"""
rxlan-ingest-weather (Step 1)
- Reads env vars: OW_API_KEY, OW_BASE_URL, CITY_LIST, UNITS, APP_NAME, STAGE
- Calls OpenWeather for each city (with retries + backoff)
- Normalizes payload (app, stage, source, fetched_at_utc, city, country, lat, lon, temp_c, feels_like_c, humidity, pressure, wind_speed, weather_main, weather_description)
- Basic validation (ranges, required fields); logs records regardless
- Prints one JSON line per city to CloudWatch
- Returns summary: {"cities": N, "ok": X, "errors": Y}
"""

import os
import json
import time
import requests
from datetime import datetime, timezone

# --- Config from environment (set these in Lambda console) ---
API_KEY = os.environ["8550d36852b3a9c7b8842159f313a66e"] 
API_URL = os.getenv("OW_BASE_URL", "https://api.openweathermap.org/data/2.5/weather")
UNITS = os.getenv("UNITS", "metric")
APP_NAME = os.getenv("APP_NAME", "rxlan")
STAGE = os.getenv("STAGE", "dev")

try:
    CITY_LIST = json.loads(os.getenv("CITY_LIST", '["Austin,US","Chicago,US"]'))
except json.JSONDecodeError:
    CITY_LIST = ["Austin,US"]

# --- Retry helper (simple, explicit) ---
def get_with_retries(url, params, retries=2, base_backoff=0.5, timeout=8):
    """
    Tries up to retries+1 times on:
      - network errors
      - HTTP 5xx
      - HTTP 429 (rate limit)
    Uses exponential backoff: 0.5s, 1.0s, 2.0s...
    """
    attempt = 0
    while True:
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            # retry on server errors or rate limit
            if resp.status_code >= 500 or resp.status_code == 429:
                raise requests.HTTPError(f"retryable status {resp.status_code}", response=resp)
            return resp
        except requests.RequestException as e:
            if attempt >= retries:
                # give up
                return e  # return the exception to handle upstream as an error
            # backoff then retry
            time.sleep(base_backoff * (2 ** attempt))
            attempt += 1

# --- Normalize into our contract + light validation ---
def normalize_weather_record(city, payload):
    fetched_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    sys = payload.get("sys", {})
    coord = payload.get("coord", {})
    main = payload.get("main", {})
    wind = payload.get("wind", {})
    wlist = payload.get("weather", [{}]) or [{}]
    w0 = wlist[0] if isinstance(wlist, list) else {}

    # Split "Austin,US" if present
    if "," in city:
        city_name, country_from_city = city.split(",", 1)
    else:
        city_name, country_from_city = city, ""

    rec = {
        "app": APP_NAME,
        "stage": STAGE,
        "source": "openweather",
        "fetched_at_utc": fetched_at_utc,
        "city": payload.get("name", city_name),
        "country": country_from_city or sys.get("country", ""),
        "lat": float(coord.get("lat", 0.0)) if coord.get("lat") is not None else None,
        "lon": float(coord.get("lon", 0.0)) if coord.get("lon") is not None else None,
        "temp_c": float(main.get("temp")) if main.get("temp") is not None else None,
        "feels_like_c": float(main.get("feels_like")) if main.get("feels_like") is not None else None,
        "humidity": int(main.get("humidity")) if main.get("humidity") is not None else None,
        "pressure": int(main.get("pressure")) if main.get("pressure") is not None else None,
        "wind_speed": float(wind.get("speed")) if wind.get("speed") is not None else None,
        "weather_main": w0.get("main"),
        "weather_description": w0.get("description"),
        # keep raw minimal to save log cost
        "raw": {"id": payload.get("id"), "dt": payload.get("dt")},
    }

    # Basic validation (don’t fail the batch)
    errors = []
    if rec["temp_c"] is None or not (-90 <= rec["temp_c"] <= 60):
        errors.append("temp_c_out_of_range_or_null")
    if rec["humidity"] is None or not (0 <= rec["humidity"] <= 100):
        errors.append("humidity_out_of_range_or_null")
    if rec["pressure"] is None or rec["pressure"] <= 0:
        errors.append("pressure_missing_or_invalid")
    if errors:
        rec["validation_errors"] = errors

    return rec

# --- Fetch + normalize per city, log one JSON per city ---
def lambda_handler(event, context):
    ok = 0
    errs = 0

    for city in CITY_LIST:
        params = {"q": city, "appid": API_KEY, "units": UNITS}
        resp = get_with_retries(API_URL, params)

        # If our helper returned an exception, count as error
        if isinstance(resp, Exception):
            print(json.dumps({"level": "error", "city": city, "exception": str(resp)}))
            errs += 1
            continue

        if resp.status_code != 200:
            print(json.dumps({
                "level": "error",
                "city": city,
                "status": resp.status_code,
                "body": resp.text[:200]
            }))
            errs += 1
            continue

        # Normalize immediately (production-style)
        payload = resp.json()
        rec = normalize_weather_record(city, payload)

        # One compact JSON line per city (CloudWatch friendly)
        print(json.dumps(rec, separators=(",", ":")))
        ok += 1

    return {"cities": len(CITY_LIST), "ok": ok, "errors": errs}

