"""
glue_job3_consolidate.py
========================
Stage 3 of the ETL pipeline.

Responsibilities:
  - Read ALL aggregated Parquet datasets from Stage 2
  - Merge them into a single, richly structured JSON document
  - JSON structure mirrors a BI-ready consolidated report
  - Write a single consolidated JSON file to S3 output bucket
  - Also write a manifest file with metadata about the run

Output JSON structure:
{
  "metadata": { run_id, generated_at, data_range, record_counts },
  "overall_kpi": { ... },
  "regional_summary": [ { region, country, metrics } ],
  "category_summary": [ { category, sub_category, metrics } ],
  "monthly_trends": [ { year, month, region, metrics } ],
  "top_products": { "<region>": [ { rank, product, metrics } ] },
  "customer_segments": { "HIGH_VALUE": count, "MID_VALUE": count, "LOW_VALUE": count },
  "payment_distribution": [ { method, region, metrics } ],
  "order_status_summary": [ { status, region, metrics } ]
}
"""

import sys
import json
import uuid
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F

import boto3

# ── Bootstrap ────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "INPUT_PATH",
    "OUTPUT_PATH",
    "OUTPUT_BUCKET",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

INPUT_PATH    = args["INPUT_PATH"]
OUTPUT_PATH   = args["OUTPUT_PATH"]
OUTPUT_BUCKET = args["OUTPUT_BUCKET"]
RUN_ID        = str(uuid.uuid4())
RUN_TS        = datetime.now(timezone.utc).isoformat()

logger.info(f"[Job3-Consolidate] Starting | run_id={RUN_ID}")

# ── Helper: read a parquet sub-dataset ───────────────────────────────────────

def read_agg(name: str):
    path = f"{INPUT_PATH}{name}/"
    logger.info(f"[Job3-Consolidate] Reading [{name}] from {path}")
    return spark.read.parquet(path)

# ── Helper: safely serialize datetime objects ─────────────────────────────────

def make_serializable(obj):
    """Recursively convert non-JSON-serializable types."""
    if isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(i) for i in obj]
    elif hasattr(obj, 'isoformat'):     # datetime / date
        return obj.isoformat()
    elif obj != obj:                    # NaN check
        return None
    else:
        return obj

# ── Helper: convert spark Row list → plain python list of dicts ───────────────

def rows_to_list(df, drop_cols=None):
    drop_cols = drop_cols or ["etl_job", "etl_ts"]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)
    return [make_serializable(row.asDict()) for row in df.collect()]

# ── Read all aggregated datasets ─────────────────────────────────────────────

df_kpi        = read_agg("overall_kpi")
df_region     = read_agg("region_country_monthly")
df_category   = read_agg("category_monthly")
df_trend      = read_agg("monthly_trend")
df_products   = read_agg("top_products_by_region")
df_customers  = read_agg("customer_segments")
df_payment    = read_agg("payment_distribution")
df_status     = read_agg("order_status_summary")

# ── Build each section ────────────────────────────────────────────────────────

# 1. Overall KPI (single row → dict)
kpi_row = df_kpi.drop("etl_job", "etl_ts").first()
overall_kpi = make_serializable(kpi_row.asDict()) if kpi_row else {}

# 2. Regional summary — aggregate across all months for final rollup
regional_summary = rows_to_list(
    df_region
    .groupBy("region", "country")
    .agg(
        F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
        F.round(F.sum("total_profit"), 2).alias("total_profit"),
        F.sum("total_units_sold").alias("total_units_sold"),
        F.sum("total_orders").alias("total_orders"),
        F.round(F.avg("avg_order_value"), 2).alias("avg_order_value"),
        F.round(F.sum("total_profit") / F.sum("total_revenue") * 100, 2).alias("profit_margin_pct"),
    )
    .orderBy(F.col("total_revenue").desc())
)

# 3. Category summary — rollup across months
category_summary = rows_to_list(
    df_category
    .groupBy("category", "sub_category")
    .agg(
        F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
        F.round(F.sum("total_profit"), 2).alias("total_profit"),
        F.sum("total_units_sold").alias("total_units_sold"),
        F.sum("total_orders").alias("total_orders"),
        F.round(F.avg("avg_discount_pct"), 2).alias("avg_discount_pct"),
        F.round(F.sum("total_profit") / F.sum("total_revenue") * 100, 2).alias("profit_margin_pct"),
    )
    .orderBy("category", F.col("total_revenue").desc())
)

# 4. Monthly trends (full granularity)
monthly_trends = rows_to_list(
    df_trend.orderBy("year", "month", "region")
)

# 5. Top products — nested by region
top_products = {}
regions = [row["region"] for row in df_products.select("region").distinct().collect()]
for region in regions:
    region_products = rows_to_list(
        df_products.filter(F.col("region") == region).orderBy("rank_in_region")
    )
    top_products[region] = region_products

# 6. Customer segment counts
seg_counts = (
    df_customers
    .groupBy("customer_segment", "customer_status")
    .agg(
        F.count("*").alias("customer_count"),
        F.round(F.sum("lifetime_value"), 2).alias("total_lifetime_value"),
        F.round(F.avg("lifetime_value"), 2).alias("avg_lifetime_value"),
        F.round(F.avg("total_orders"), 2).alias("avg_orders_per_customer"),
    )
    .orderBy("customer_segment", "customer_status")
)
customer_segments = rows_to_list(seg_counts)

# Segment totals
segment_totals = {}
for row in df_customers.groupBy("customer_segment").count().collect():
    segment_totals[row["customer_segment"]] = row["count"]

# 7. Payment distribution — rollup
payment_distribution = rows_to_list(
    df_payment
    .groupBy("payment_method", "region")
    .agg(
        F.sum("transaction_count").alias("transaction_count"),
        F.round(F.sum("total_revenue"), 2).alias("total_revenue"),
        F.round(F.avg("avg_transaction_value"), 2).alias("avg_transaction_value"),
    )
    .orderBy("region", F.col("total_revenue").desc())
)

# 8. Order status summary — rollup
order_status_summary = rows_to_list(
    df_status
    .groupBy("status", "region")
    .agg(
        F.sum("order_count").alias("order_count"),
        F.round(F.sum("revenue_at_risk"), 2).alias("total_revenue"),
    )
    .orderBy("region", "status")
)

# ── Assemble consolidated JSON ────────────────────────────────────────────────

consolidated = {
    "metadata": {
        "run_id":           RUN_ID,
        "generated_at":     RUN_TS,
        "etl_job":          args["JOB_NAME"],
        "data_range": {
            "from": overall_kpi.get("data_from"),
            "to":   overall_kpi.get("data_to"),
        },
        "record_counts": {
            "regional_rows":    len(regional_summary),
            "category_rows":    len(category_summary),
            "monthly_trend_rows": len(monthly_trends),
            "top_product_rows": sum(len(v) for v in top_products.values()),
            "customer_rows":    len(customer_segments),
            "payment_rows":     len(payment_distribution),
            "status_rows":      len(order_status_summary),
        },
        "customer_segment_totals": segment_totals,
    },
    "overall_kpi":         overall_kpi,
    "regional_summary":    regional_summary,
    "category_summary":    category_summary,
    "monthly_trends":      monthly_trends,
    "top_products":        top_products,
    "customer_segments":   customer_segments,
    "payment_distribution": payment_distribution,
    "order_status_summary": order_status_summary,
}

# ── Write JSON to S3 ──────────────────────────────────────────────────────────

json_str     = json.dumps(consolidated, indent=2, default=str)
date_prefix  = datetime.now(timezone.utc).strftime("%Y/%m/%d")
s3_key       = f"consolidated/{date_prefix}/etl_report_{RUN_ID}.json"
manifest_key = f"consolidated/{date_prefix}/manifest_{RUN_ID}.json"

s3_client = boto3.client("s3")

# Upload main consolidated JSON
logger.info(f"[Job3-Consolidate] Writing JSON to s3://{OUTPUT_BUCKET}/{s3_key}")
s3_client.put_object(
    Bucket      = OUTPUT_BUCKET,
    Key         = s3_key,
    Body        = json_str.encode("utf-8"),
    ContentType = "application/json",
    Metadata    = {
        "run-id":       RUN_ID,
        "generated-at": RUN_TS,
    }
)

# Upload manifest
manifest = {
    "run_id":       RUN_ID,
    "generated_at": RUN_TS,
    "s3_output":    f"s3://{OUTPUT_BUCKET}/{s3_key}",
    "file_size_bytes": len(json_str.encode("utf-8")),
    "status":       "SUCCESS",
    "record_counts": consolidated["metadata"]["record_counts"],
}

s3_client.put_object(
    Bucket      = OUTPUT_BUCKET,
    Key         = manifest_key,
    Body        = json.dumps(manifest, indent=2).encode("utf-8"),
    ContentType = "application/json",
)

logger.info(f"[Job3-Consolidate] Manifest written to s3://{OUTPUT_BUCKET}/{manifest_key}")
logger.info(f"[Job3-Consolidate] JSON size: {len(json_str):,} bytes")
logger.info(f"[Job3-Consolidate] COMPLETE | run_id={RUN_ID}")

job.commit()
