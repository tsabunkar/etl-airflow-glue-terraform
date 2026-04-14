"""
glue_job1_clean.py
==================
Stage 1 of the ETL pipeline.

Responsibilities:
  - Read raw CSV files from S3 (supports large multi-partition CSVs)
  - Infer / enforce schema
  - Cleanse: trim whitespace, normalize date formats, cast types
  - Validate: drop rows missing critical fields, flag bad records
  - Deduplicate on composite natural key
  - Write cleaned output as Parquet (partitioned by year/month)
  - Write bad records to a quarantine path for inspection

Input  : s3://<RAW_BUCKET>/input/**/*.csv
Output : s3://<CLEANED_BUCKET>/cleaned/year=YYYY/month=MM/
"""

import sys
import re
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, LongType
)

# ── Bootstrap ────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "INPUT_PATH",
    "OUTPUT_PATH",
    "RAW_BUCKET",
    "CLEANED_BUCKET",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info(f"[Job1-Clean] Starting | input={args['INPUT_PATH']}")

# ── Configuration ────────────────────────────────────────────────────────────

INPUT_PATH    = args["INPUT_PATH"]
OUTPUT_PATH   = args["OUTPUT_PATH"]
QUARANTINE    = f"s3://{args['CLEANED_BUCKET']}/quarantine/"

# Critical fields — rows missing any of these are quarantined
REQUIRED_FIELDS = ["order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price"]

# Natural dedup key
DEDUP_KEY = ["order_id", "product_id"]

# ── Explicit Schema (avoids full CSV scan for type inference) ─────────────────

RAW_SCHEMA = StructType([
    StructField("order_id",        StringType(),  True),
    StructField("customer_id",     StringType(),  True),
    StructField("product_id",      StringType(),  True),
    StructField("product_name",    StringType(),  True),
    StructField("category",        StringType(),  True),
    StructField("sub_category",    StringType(),  True),
    StructField("region",          StringType(),  True),
    StructField("country",         StringType(),  True),
    StructField("order_date",      StringType(),  True),   # parsed below
    StructField("ship_date",       StringType(),  True),
    StructField("quantity",        StringType(),  True),   # cast below
    StructField("unit_price",      StringType(),  True),
    StructField("discount",        StringType(),  True),
    StructField("sales_amount",    StringType(),  True),
    StructField("profit",          StringType(),  True),
    StructField("shipping_cost",   StringType(),  True),
    StructField("payment_method",  StringType(),  True),
    StructField("status",          StringType(),  True),
])

# ── Step 1: Read raw CSV ──────────────────────────────────────────────────────

logger.info("[Job1-Clean] Reading raw CSV files …")

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "false")
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .schema(RAW_SCHEMA)
    .csv(INPUT_PATH)
)

total_raw = raw_df.count()
logger.info(f"[Job1-Clean] Raw records read: {total_raw:,}")

# ── Step 2: Trim all string columns ──────────────────────────────────────────

string_cols = [f.name for f in raw_df.schema.fields if isinstance(f.dataType, StringType)]

trimmed_df = raw_df
for col in string_cols:
    trimmed_df = trimmed_df.withColumn(col, F.trim(F.col(col)))

# Replace empty strings with null
for col in string_cols:
    trimmed_df = trimmed_df.withColumn(
        col, F.when(F.col(col) == "", None).otherwise(F.col(col))
    )

# ── Step 3: Validate required fields ─────────────────────────────────────────

validation_condition = F.lit(True)
for field in REQUIRED_FIELDS:
    validation_condition = validation_condition & F.col(field).isNotNull()

valid_df      = trimmed_df.filter(validation_condition)
quarantine_df = trimmed_df.filter(~validation_condition) \
                           .withColumn("quarantine_reason", F.lit("missing_required_field")) \
                           .withColumn("quarantine_ts", F.current_timestamp())

bad_count = quarantine_df.count()
logger.info(f"[Job1-Clean] Quarantined (missing required): {bad_count:,}")

# ── Step 4: Type casting ──────────────────────────────────────────────────────

# Date parsing — handle multiple date formats
def parse_date_col(df, col_name):
    return df.withColumn(
        col_name,
        F.coalesce(
            F.to_timestamp(F.col(col_name), "yyyy-MM-dd"),
            F.to_timestamp(F.col(col_name), "MM/dd/yyyy"),
            F.to_timestamp(F.col(col_name), "dd-MM-yyyy"),
            F.to_timestamp(F.col(col_name), "yyyy/MM/dd"),
        )
    )

casted_df = (
    valid_df
    .withColumn("quantity",      F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price",    F.col("unit_price").cast(DoubleType()))
    .withColumn("discount",      F.col("discount").cast(DoubleType()))
    .withColumn("sales_amount",  F.col("sales_amount").cast(DoubleType()))
    .withColumn("profit",        F.col("profit").cast(DoubleType()))
    .withColumn("shipping_cost", F.col("shipping_cost").cast(DoubleType()))
)

casted_df = parse_date_col(casted_df, "order_date")
casted_df = parse_date_col(casted_df, "ship_date")

# ── Step 5: Business rule validation ─────────────────────────────────────────

# Recalculate sales_amount = quantity * unit_price * (1 - discount)
casted_df = casted_df.withColumn(
    "sales_amount_calc",
    F.round(
        F.col("quantity") * F.col("unit_price") * (1 - F.coalesce(F.col("discount"), F.lit(0.0))),
        2
    )
)

# Flag rows where provided sales_amount deviates >1% from calculated
casted_df = casted_df.withColumn(
    "sales_amount_flag",
    F.when(
        F.abs(F.col("sales_amount") - F.col("sales_amount_calc")) / F.col("sales_amount_calc") > 0.01,
        F.lit("MISMATCH")
    ).otherwise(F.lit("OK"))
)

# Quarantine negative quantities or prices
invalid_numeric = casted_df.filter(
    (F.col("quantity") <= 0) | (F.col("unit_price") <= 0)
)
valid_numeric = casted_df.filter(
    (F.col("quantity") > 0) & (F.col("unit_price") > 0)
)

# Union quarantine sets
quarantine_full = quarantine_df.select("order_id", "quarantine_reason", "quarantine_ts") \
    .union(
        invalid_numeric
        .withColumn("quarantine_reason", F.lit("invalid_numeric"))
        .withColumn("quarantine_ts", F.current_timestamp())
        .select("order_id", "quarantine_reason", "quarantine_ts")
    )

# ── Step 6: Deduplication ─────────────────────────────────────────────────────

from pyspark.sql.window import Window

dedup_window = Window.partitionBy(DEDUP_KEY).orderBy(F.col("order_date").desc())

deduped_df = (
    valid_numeric
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

# ── Step 7: Add audit columns ─────────────────────────────────────────────────

final_df = (
    deduped_df
    .withColumn("etl_job",          F.lit(args["JOB_NAME"]))
    .withColumn("etl_processed_at", F.current_timestamp())
    .withColumn("year",             F.year(F.col("order_date")))
    .withColumn("month",            F.month(F.col("order_date")))
)

clean_count = final_df.count()
logger.info(f"[Job1-Clean] Clean records after dedup: {clean_count:,}")
logger.info(f"[Job1-Clean] Dropped/quarantined: {total_raw - clean_count:,}")

# ── Step 8: Write cleaned Parquet (partitioned) ───────────────────────────────

logger.info(f"[Job1-Clean] Writing cleaned Parquet to {OUTPUT_PATH}")

(
    final_df
    .repartition(20, "year", "month")
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(OUTPUT_PATH)
)

# Write quarantine records
if quarantine_full.count() > 0:
    logger.info(f"[Job1-Clean] Writing quarantine records to {QUARANTINE}")
    (
        quarantine_full
        .write
        .mode("append")
        .parquet(QUARANTINE)
    )

# ── Step 9: Emit summary metrics to CloudWatch (via logger) ───────────────────

logger.info(f"[Job1-Clean] SUMMARY | total_raw={total_raw} | clean={clean_count} | quarantine={total_raw - clean_count}")

job.commit()
logger.info("[Job1-Clean] Job completed successfully.")
