"""
glue_job2_aggregate.py
======================
Stage 2 of the ETL pipeline.

Responsibilities:
  - Read cleaned Parquet from Stage 1
  - Compute KPI aggregations across multiple dimensions:
      * Revenue, profit, quantity by region / country
      * Revenue, profit by product category / sub-category
      * Monthly revenue trend per region
      * Top-10 products by revenue per region
      * Customer segment analysis (high/medium/low value)
      * Payment method distribution
      * Average order value (AOV) per region
  - Write aggregated datasets as Parquet, each in its own prefix

Input  : s3://<CLEANED_BUCKET>/cleaned/**
Output : s3://<PROCESSED_BUCKET>/aggregated/<dataset>/
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Bootstrap ────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "INPUT_PATH",
    "OUTPUT_PATH",
    "PROCESSED_BUCKET",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info(f"[Job2-Aggregate] Starting | input={args['INPUT_PATH']}")

INPUT_PATH  = args["INPUT_PATH"]
OUTPUT_PATH = args["OUTPUT_PATH"]

# ── Load cleaned data ─────────────────────────────────────────────────────────

df = spark.read.parquet(INPUT_PATH)
df.cache()

total = df.count()
logger.info(f"[Job2-Aggregate] Loaded {total:,} clean records.")

# ── Helper: write a dataframe to a named sub-prefix ──────────────────────────

def write_agg(agg_df, name: str, partitions: list = None):
    path = f"{OUTPUT_PATH}{name}/"
    logger.info(f"[Job2-Aggregate] Writing [{name}] → {path}")
    writer = agg_df.coalesce(5).write.mode("overwrite")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.parquet(path)

# ── Aggregation 1: Revenue & Profit by Region & Country ──────────────────────

agg_region = (
    df.groupBy("region", "country", "year", "month")
    .agg(
        F.round(F.sum("sales_amount"), 2).alias("total_revenue"),
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.avg("sales_amount"), 2).alias("avg_order_value"),
        F.round(F.sum("profit") / F.sum("sales_amount") * 100, 2).alias("profit_margin_pct"),
    )
    .withColumn("etl_job", F.lit(args["JOB_NAME"]))
    .withColumn("etl_ts", F.current_timestamp())
)

write_agg(agg_region, "region_country_monthly", partitions=["year", "month"])

# ── Aggregation 2: Revenue by Product Category & Sub-Category ────────────────

agg_category = (
    df.groupBy("category", "sub_category", "year", "month")
    .agg(
        F.round(F.sum("sales_amount"), 2).alias("total_revenue"),
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.avg("discount") * 100, 2).alias("avg_discount_pct"),
        F.round(F.sum("profit") / F.sum("sales_amount") * 100, 2).alias("profit_margin_pct"),
    )
    .withColumn("etl_job", F.lit(args["JOB_NAME"]))
    .withColumn("etl_ts", F.current_timestamp())
)

write_agg(agg_category, "category_monthly", partitions=["year", "month"])

# ── Aggregation 3: Monthly Revenue Trend ─────────────────────────────────────

agg_monthly_trend = (
    df.groupBy("year", "month", "region")
    .agg(
        F.round(F.sum("sales_amount"), 2).alias("monthly_revenue"),
        F.round(F.sum("profit"), 2).alias("monthly_profit"),
        F.sum("quantity").alias("monthly_units"),
        F.countDistinct("order_id").alias("monthly_orders"),
    )
    .orderBy("year", "month", "region")
    .withColumn("etl_job", F.lit(args["JOB_NAME"]))
    .withColumn("etl_ts", F.current_timestamp())
)

# Add month-over-month revenue growth %
mom_window = Window.partitionBy("region").orderBy("year", "month")
agg_monthly_trend = (
    agg_monthly_trend
    .withColumn("prev_month_revenue", F.lag("monthly_revenue", 1).over(mom_window))
    .withColumn(
        "mom_growth_pct",
        F.when(
            F.col("prev_month_revenue").isNotNull() & (F.col("prev_month_revenue") > 0),
            F.round(
                (F.col("monthly_revenue") - F.col("prev_month_revenue"))
                / F.col("prev_month_revenue") * 100, 2
            )
        ).otherwise(None)
    )
    .drop("prev_month_revenue")
)

write_agg(agg_monthly_trend, "monthly_trend")

# ── Aggregation 4: Top Products by Revenue per Region ────────────────────────

agg_product = (
    df.groupBy("region", "product_id", "product_name", "category", "sub_category")
    .agg(
        F.round(F.sum("sales_amount"), 2).alias("total_revenue"),
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

# Rank within each region
rank_window = Window.partitionBy("region").orderBy(F.col("total_revenue").desc())

agg_top_products = (
    agg_product
    .withColumn("rank_in_region", F.rank().over(rank_window))
    .filter(F.col("rank_in_region") <= 10)
    .withColumn("etl_job", F.lit(args["JOB_NAME"]))
    .withColumn("etl_ts", F.current_timestamp())
)

write_agg(agg_top_products, "top_products_by_region")

# ── Aggregation 5: Customer Value Segmentation ────────────────────────────────

agg_customer = (
    df.groupBy("customer_id", "region", "country")
    .agg(
        F.round(F.sum("sales_amount"), 2).alias("lifetime_value"),
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.avg("sales_amount"), 2).alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.round(F.sum("profit"), 2).alias("total_profit_generated"),
    )
    .withColumn(
        "customer_segment",
        F.when(F.col("lifetime_value") >= 5000, F.lit("HIGH_VALUE"))
         .when(F.col("lifetime_value") >= 1000, F.lit("MID_VALUE"))
         .otherwise(F.lit("LOW_VALUE"))
    )
    .withColumn(
        "days_since_last_order",
        F.datediff(F.current_date(), F.col("last_order_date").cast("date"))
    )
    .withColumn(
        "customer_status",
        F.when(F.col("days_since_last_order") <= 90,  F.lit("ACTIVE"))
         .when(F.col("days_since_last_order") <= 365, F.lit("AT_RISK"))
         .otherwise(F.lit("CHURNED"))
    )
    .withColumn("etl_job", F.lit(args["JOB_NAME"]))
    .withColumn("etl_ts", F.current_timestamp())
)

write_agg(agg_customer, "customer_segments")

# ── Aggregation 6: Payment Method Distribution ────────────────────────────────

agg_payment = (
    df.groupBy("payment_method", "region", "year", "month")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("sales_amount"), 2).alias("total_revenue"),
        F.round(F.avg("sales_amount"), 2).alias("avg_transaction_value"),
    )
    .withColumn("etl_job", F.lit(args["JOB_NAME"]))
    .withColumn("etl_ts", F.current_timestamp())
)

write_agg(agg_payment, "payment_distribution", partitions=["year", "month"])

# ── Aggregation 7: Order Status Summary ───────────────────────────────────────

agg_status = (
    df.groupBy("status", "region", "year", "month")
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.sum("sales_amount"), 2).alias("revenue_at_risk"),
    )
    .withColumn("etl_job", F.lit(args["JOB_NAME"]))
    .withColumn("etl_ts", F.current_timestamp())
)

write_agg(agg_status, "order_status_summary", partitions=["year", "month"])

# ── Aggregation 8: Overall KPI Summary ────────────────────────────────────────

agg_kpi = (
    df.agg(
        F.round(F.sum("sales_amount"), 2).alias("grand_total_revenue"),
        F.round(F.sum("profit"), 2).alias("grand_total_profit"),
        F.round(F.avg("profit") / F.avg("sales_amount") * 100, 2).alias("overall_profit_margin_pct"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product_id").alias("unique_products"),
        F.round(F.avg("sales_amount"), 2).alias("avg_order_value"),
        F.min("order_date").alias("data_from"),
        F.max("order_date").alias("data_to"),
    )
    .withColumn("generated_at", F.current_timestamp())
    .withColumn("etl_job", F.lit(args["JOB_NAME"]))
)

write_agg(agg_kpi, "overall_kpi")

df.unpersist()

logger.info("[Job2-Aggregate] All aggregations written successfully.")
job.commit()
