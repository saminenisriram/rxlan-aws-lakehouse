# Weather Data Pipeline – README

A small, end-to-end data pipeline that fetches current weather for a list of cities, stores normalized records in DynamoDB, fans out changes to S3 (Bronze) via Kinesis + Firehose, and loads analytics-ready data to Amazon Redshift using AWS Glue.

---

## Architecture (high level)

1. **Lambda (Fetcher)** → calls OpenWeather (or similar) for a list of cities and emits **normalized JSON**.
2. **DynamoDB (Table)** ← Fetcher **PutItem** per city + timestamp (Step 2).
3. **DynamoDB Streams** → **Lambda (Forwarder)** → **Kinesis Data Streams** → **Firehose** → **S3 Bronze** as compressed **NDJSON .gz** in partitions `raw/openweather/dt=YYYY-MM-DD/hour=HH/` (Step 3).
4. **AWS Glue (ETL Job)** → reads Bronze, normalizes/flattens, and **loads to Amazon Redshift** into `public.weather` (Step 4).

```
[Fetcher Lambda] → DynamoDB → (Streams) → Forwarder Lambda → Kinesis → Firehose → S3 (Bronze)
                                                                     ↓
                                                              Glue (ETL) → Redshift (dev.public.weather)
```

---

## Repo layout

```
.
├─ lambdas/
│  ├─ fetch_weather/               # Step 2: put normalized items into DynamoDB
│  └─ ddb_stream_forwarder/        # Step 3: DDB Streams → Kinesis
├─ glue/
│  └─ rxlan_bronze_to_redshift_openweather.py   # Step 4: S3 → Redshift ETL
├─ infra/                          # (optional) IaC templates for roles, streams, firehose, etc.
└─ README.md
```

---

## Data model

**Normalized weather record** (what goes to DynamoDB and then to S3):

```json
{
  "app": "rxlan",
  "stage": "dev",
  "source": "openweather",
  "fetched_at_utc": "2025-08-28T15:27:30Z",
  "city": "Austin",
  "country": "US",
  "lat": 30.2672,
  "lon": -97.7431,
  "temp_c": 35.77,
  "feels_like_c": 40.12,
  "humidity": 44,
  "pressure": 1015,
  "wind_speed": 3.13,
  "weather_main": "Clear",
  "weather_description": "clear sky",
  "raw": { "id": 4671654, "dt": 1756394854 }    // optional passthrough
}
```

**DynamoDB keys**:

* `pk = "CITY#<CityName>"`
* `sk = "TS#<ISO8601 timestamp>"`

**S3 Bronze layout**:

```
s3://rxlan-bronze-dev/raw/openweather/dt=YYYY-MM-DD/hour=HH/part-*.json.gz
```

**Redshift table** (created by Glue if missing):

```
dev.public.weather(
  app, stage, source, fetched_at_utc, ts, city, country, lat, lon,
  temp_c, feels_like_c, humidity, pressure, wind_speed,
  dt, hour, loaded_at
)
```

---

## Step 2 — DynamoDB table + Fetcher Lambda (PutItem)

### 2.1 Create the table

* **Table name**: `weather_events`
* **Partition key**: `pk` (String)
* **Sort key**: `sk` (String)
* **Streams**: **Disabled** for now (we enable in Step 3)

### 2.2 Lambda (Fetcher) outline

* Fetch **8 cities** by default:

  ```json
  ["Austin,US","Chicago,US","Phoenix,US","Dallas,US","Houston,US","London,UK","New York,US","Tokyo,JP"]
  ```
* Normalize to the JSON shape above.
* Put one item per city to DynamoDB.

**Minimal IAM for Fetcher Lambda** (attach to the function’s role):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["dynamodb:PutItem"], "Resource": "arn:aws:dynamodb:<REGION>:<ACCOUNT>:table/weather_events" },
    { "Effect": "Allow", "Action": ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"], "Resource": "*" }
  ]
}
```

> **Why**: `PutItem` writes normalized records. CloudWatch Logs is for observability.

---

## Step 3 — Streams → Forwarder Lambda → Kinesis → Firehose → S3 (Bronze)

### 3.1 Turn on Streams

* DynamoDB → `weather_events` → **Exports and streams** → **Enable streams**
* **Stream view type**: `NEW_IMAGE`

### 3.2 Forwarder Lambda (given)

* Trigger: **DynamoDB Streams** (source table above)
* Code: takes `INSERT` events, deserializes `NewImage`, sends to **Kinesis Data Streams** (`rxlan-ddb-events-dev`).

**Minimal IAM for Forwarder Lambda**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["kinesis:PutRecords"], "Resource": "arn:aws:kinesis:<REGION>:<ACCOUNT>:stream/rxlan-ddb-events-dev" },
    { "Effect": "Allow", "Action": ["dynamodb:DescribeStream","dynamodb:GetRecords","dynamodb:GetShardIterator","dynamodb:ListStreams"], "Resource": "*" },
    { "Effect": "Allow", "Action": ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"], "Resource": "*" }
  ]
}
```

### 3.3 Kinesis Data Streams → Firehose → S3

* **Source**: Kinesis Data Stream 
* **Destination**: S3 bucket name
* **S3 prefix**: `raw/openweather/dt=!{timestamp:yyyy-MM-dd}/hour=!{timestamp:HH}/`
* **Buffering**: e.g., 1–5 MB or 60 sec
* **Compression**: GZIP
* (Optional) Firehose record transformation disabled (we already normalized)

> Firehose writes compressed NDJSON files under the date/hour partitions.

---

## Step 4 — Glue ETL job → Redshift

### 4.1 Glue connection to Redshift

* Create a **JDBC Redshift connection** (console).
* Store DB credentials in the connection (Secrets Manager).
* Test the connection (VPC/subnets/SG must allow access to Redshift serverless/workgroup).

### 4.2 Glue job (script)

Use **`glue/rxlan_bronze_to_redshift_openweather.py`** (already in repo).
Job parameters (example):

```
--bronze_path         s3://rxlan-bronze-dev/raw/openweather/
--redshift_connection Redshift connection
--redshift_database   dev
--redshift_schema     public
--redshift_table      weather
--redshift_temp_dir   s3://aws-glue-assets-<ACCOUNT>-us-west-1/temporary/
```

**Recommended job settings**

* Glue version: 4.0 or 5.0
* Workers: 2–5 (depends on volume)
* **Max concurrent runs = 1**
* Retries: 1–2
* Timeout: 15–30 min
* **Schedule**: **Hourly** (job schedule)
  The script defaults to reading the **last completed UTC hour** if no filters are passed.

> You can also pass `--dt_filter` and `--hour_filter` for ad-hoc backfills.

### 4.3 Verify in Redshift

Latest rows:

```sql
SELECT *
FROM public.weather
ORDER BY loaded_at DESC
LIMIT 50;
```

Last hour’s load:

```sql
SELECT dt, hour, COUNT(*) AS rows_loaded, MAX(loaded_at) AS last_load
FROM public.weather
WHERE dt   = TO_CHAR(DATEADD(hour, -1, GETDATE()), 'YYYY-MM-DD')
  AND hour = TO_CHAR(DATEADD(hour, -1, GETDATE()), 'HH24')
GROUP BY 1,2
ORDER BY 1,2;
```

---

## Running on a schedule

**Option used here**: Glue job **hourly schedule** (no extra infra).
The job auto-targets `dt=<last-UTC-day>/hour=<last-UTC-hour>` so each run is fast and idempotent.

> Alternative: EventBridge rule on **S3 Object Created** under `raw/openweather/` to trigger the job on arrival (not required since you opted for hourly schedule).

---

## Configuration

### Cities list (Fetcher Lambda)

* Keep your default 8 cities:

  ```json
  ["Austin,US","Chicago,US","Phoenix,US","Dallas,US","Houston,US","London,UK","New York,US","Tokyo,JP"]
  ```
* To add more, update the Lambda env var `CITIES` (comma-separated) or your code list.

### Environment variables (typical)

Fetcher Lambda:

* `OPENWEATHER_API_KEY`
* `TABLE_NAME=weather_events`
* `CITIES=Austin,US,Chicago,US,...`

Forwarder Lambda:

* `KINESIS_STREAM=rxlan-ddb-dev`
* `PARTITION_KEY=city` (optional)

---

## Observability & Ops

* **CloudWatch Logs**:

  * Fetcher Lambda: external API errors, per-city success/failure.
  * Forwarder Lambda: batch counts, failed Kinesis records.
  * Firehose: delivery/processing errors.
  * Glue Job: input/good row counts, Redshift load success.
* **Alarms** (recommended):

  * Glue job failed runs ≥ 1 in 1 hour → SNS/email.
  * Firehose delivery errors.
* **Data quality** (optional later):

  * Add simple checks in Glue: e.g., drop rows where `city` is null, or `temp_c` outside plausible ranges.

---

## Security notes

* Redshift credentials are stored in the **Glue Connection** (Secrets Manager). No secrets in code.
* IAM policies are **minimal** and scoped to resources (table, stream, bucket).
* VPC access is needed if Redshift is in a VPC; ensure the Glue job role subnets/SG allow egress to Redshift and S3.

---

## Cost tips

* Keep Firehose buffers modest to reduce object churn (fewer files).
* Glue job reads a **single hour** per run → smaller scan.
* Consider Redshift **automatic table optimization** (dist/sort) as data grows.

---

## Troubleshooting quickies

* **Nulls in Redshift**: Usually means JSON wasn’t expanded or wrong column names. This repo’s Glue script:

  * auto-detects `data`/`payload` string columns,
  * infers the schema from a sample,
  * **expands into columns** before load.
* **No new S3 files**: Check Firehose delivery logs and DynamoDB → Streams metrics.
* **Glue connection fails**: VPC routes/SGs, Secrets Manager access for the job role.




