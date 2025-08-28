import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

# --------- Required job args ----------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_path",          # s3://rxlan-bronze-dev/raw/openweather/
    "redshift_connection",  # Glue Connection name
    "redshift_database",    # dev
    "redshift_schema",      # public
    "redshift_table",       # weather
    "redshift_temp_dir"     # s3://<bucket>/glue-redshift-temp/
])

bronze_path       = args["bronze_path"].rstrip("/") + "/"
conn_name         = args["redshift_connection"]
rs_db             = args["redshift_database"]
rs_schema         = args["redshift_schema"]
rs_table          = args["redshift_table"]
redshift_temp_dir = args["redshift_temp_dir"].rstrip("/")

# --------- Spark/Glue ----------
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
log = glue.get_logger()

log.info(f"Starting job. bronze={bronze_path} target={rs_db}.{rs_schema}.{rs_table}")

# --------- Read Bronze (recursive) ----------
df = (
    spark.read
         .option("recursiveFileLookup", "true")
         .json(bronze_path)  # handles .gz NDJSON too
)

# --------- Expand JSON string column (data/payload) ----------
dtypes = dict(df.dtypes)
json_col = None
if "payload" in df.columns and dtypes.get("payload") == "string":
    json_col = "payload"
elif "data" in df.columns and dtypes.get("data") == "string":
    json_col = "data"

if json_col:
    sample = df.filter(F.col(json_col).isNotNull()).limit(200).select(json_col)
    if sample.count() > 0:
        inferred_schema = spark.read.json(sample.rdd.map(lambda r: r[0])).schema
        df = (
            df.withColumn("_j", F.from_json(F.col(json_col), inferred_schema))
              .select("_j.*")   # keep only expanded JSON fields
        )
    else:
        log.info(f"`{json_col}` had no non-null sample rows; nothing to expand.")
else:
    log.info("No string JSON column named `data` or `payload`; assuming columns already expanded.")

input_count = df.count()
log.info(f"Read {input_count} rows from Bronze")
if input_count == 0:
    log.info("No data found. Exiting.")
    sys.exit(0)

# --------- Normalize / cast ----------
expected_cols = {
    "app": StringType(),
    "stage": StringType(),
    "source": StringType(),
    "fetched_at_utc": StringType(),
    "city": StringType(),
    "country": StringType(),
    "lat": DoubleType(),
    "lon": DoubleType(),
    "temp_c": DoubleType(),
    "feels_like_c": DoubleType(),
    "humidity": IntegerType(),
    "pressure": IntegerType(),
    "wind_speed": DoubleType(),
}
for col, typ in expected_cols.items():
    df = df.withColumn(col, F.col(col).cast(typ)) if col in df.columns else df.withColumn(col, F.lit(None).cast(typ))

# Parse timestamp like 2025-08-28T15:27:30Z
df = df.withColumn("ts", F.to_timestamp("fetched_at_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
df = df.withColumn("dt",   F.date_format(F.col("ts"), "yyyy-MM-dd"))
df = df.withColumn("hour", F.date_format(F.col("ts"), "HH"))

df_out = (
    df.select(
        "app","stage","source",
        "fetched_at_utc","ts",
        "city","country","lat","lon",
        "temp_c","feels_like_c","humidity","pressure","wind_speed",
        "dt","hour"
    )
    .withColumn("loaded_at", F.current_timestamp())
)

good_count = df_out.count()
log.info(f"Prepared {good_count} rows for Redshift {rs_schema}.{rs_table}")
if good_count == 0:
    log.info("Nothing to load after normalization. Exiting.")
    sys.exit(0)

# --------- Create table if missing; then load ----------
create_sql = f"""
CREATE TABLE IF NOT EXISTS {rs_schema}.{rs_table} (
    app            VARCHAR(64),
    stage          VARCHAR(32),
    source         VARCHAR(64),
    fetched_at_utc VARCHAR(64),
    ts             TIMESTAMP,
    city           VARCHAR(128),
    country        VARCHAR(8),
    lat            DOUBLE PRECISION,
    lon            DOUBLE PRECISION,
    temp_c         DOUBLE PRECISION,
    feels_like_c   DOUBLE PRECISION,
    humidity       INTEGER,
    pressure       INTEGER,
    wind_speed     DOUBLE PRECISION,
    dt             VARCHAR(10),
    hour           VARCHAR(2),
    loaded_at      TIMESTAMP
);
"""
post_sql = f"ANALYZE {rs_schema}.{rs_table};"

dyf = DynamicFrame.fromDF(df_out, glue, "weather")

conn_options = {
    "dbtable": f"{rs_schema}.{rs_table}",
    "database": rs_db,
    "preactions": create_sql,
    "postactions": post_sql
}

glue.write_dynamic_frame.from_jdbc_conf(
    frame=dyf,
    catalog_connection=conn_name,
    connection_options=conn_options,
    redshift_tmp_dir=redshift_temp_dir
)

log.info("Redshift load complete.")
