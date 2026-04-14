"""
etl_pipeline_dag.py
===================
Apache Airflow DAG that orchestrates the full ETL pipeline:

  1. S3 sensor — wait for new CSV files in the raw bucket
  2. Glue Crawler — crawl raw CSVs and update the Data Catalog
  3. Glue Job 1 — clean & validate
  4. Glue Job 2 — aggregate & transform
  5. Glue Job 3 — consolidate → JSON
  6. SNS notify  — alert on success or failure

DAG schedule: daily at 02:00 UTC
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ── Config (sourced from Airflow Variables or env) ────────────────────────────

ENV                = Variable.get("environment",        default_var="dev")
PROJECT            = Variable.get("project_name",       default_var="etl-poc")
AWS_REGION         = Variable.get("aws_region",         default_var="us-east-1")
AWS_CONN_ID        = Variable.get("aws_conn_id",        default_var="aws_default")

RAW_BUCKET         = Variable.get("raw_bucket")
CLEANED_BUCKET     = Variable.get("cleaned_bucket")
PROCESSED_BUCKET   = Variable.get("processed_bucket")
OUTPUT_BUCKET      = Variable.get("output_bucket")
GLUE_SCRIPTS_BUCKET = Variable.get("glue_scripts_bucket")

GLUE_ROLE_ARN      = Variable.get("glue_role_arn")
SNS_TOPIC_ARN      = Variable.get("sns_topic_arn",      default_var="")

GLUE_JOB_CLEAN         = f"{PROJECT}-{ENV}-job1-clean"
GLUE_JOB_AGGREGATE     = f"{PROJECT}-{ENV}-job2-aggregate"
GLUE_JOB_CONSOLIDATE   = f"{PROJECT}-{ENV}-job3-consolidate"
GLUE_CRAWLER_NAME      = f"{PROJECT}-{ENV}-raw-crawler"

# Shared Glue job arguments
GLUE_BASE_ARGS = {
    "--RAW_BUCKET":       RAW_BUCKET,
    "--CLEANED_BUCKET":   CLEANED_BUCKET,
    "--PROCESSED_BUCKET": PROCESSED_BUCKET,
    "--OUTPUT_BUCKET":    OUTPUT_BUCKET,
}

# ── Default DAG args ──────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["data-team@company.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id          = f"etl_pipeline_{ENV}",
    description     = "ETL pipeline: raw CSV → Glue clean → aggregate → consolidated JSON",
    default_args    = default_args,
    schedule_interval = "0 2 * * *",   # daily at 02:00 UTC
    start_date      = days_ago(1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["etl", "glue", "s3", ENV],
    doc_md          = __doc__,
) as dag:

    # ── 1. Start marker ───────────────────────────────────────────────────────

    start = EmptyOperator(task_id="start")

    # ── 2. S3 Sensor — wait for CSV files in today's partition ────────────────

    s3_sensor = S3KeySensor(
        task_id          = "wait_for_csv_files",
        bucket_name      = RAW_BUCKET,
        bucket_key       = "input/",
        wildcard_match   = True,
        aws_conn_id      = AWS_CONN_ID,
        poke_interval    = 60,          # check every 60 seconds
        timeout          = 3600,        # give up after 1 hour
        mode             = "reschedule",
        soft_fail        = False,
        doc_md           = "Wait until at least one CSV file exists in the raw S3 bucket input prefix.",
    )

    # ── 3. Glue Crawler ───────────────────────────────────────────────────────

    run_crawler = GlueCrawlerOperator(
        task_id      = "run_glue_crawler",
        config       = {"Name": GLUE_CRAWLER_NAME},
        aws_conn_id  = AWS_CONN_ID,
        poll_interval = 30,
        doc_md       = "Run the Glue crawler to update the Data Catalog schema from raw CSVs.",
    )

    # ── 4. Glue Job 1 — Clean & Validate ─────────────────────────────────────

    ds_nodash = "{{ ds_nodash }}"   # e.g. 20240415

    job1_clean = GlueJobOperator(
        task_id        = "glue_job1_clean_validate",
        job_name       = GLUE_JOB_CLEAN,
        script_args    = {
            **GLUE_BASE_ARGS,
            "--INPUT_PATH":  f"s3://{RAW_BUCKET}/input/",
            "--OUTPUT_PATH": f"s3://{CLEANED_BUCKET}/cleaned/",
        },
        aws_conn_id    = AWS_CONN_ID,
        region_name    = AWS_REGION,
        wait_for_completion = True,
        verbose        = True,
        doc_md         = "Clean, validate, deduplicate raw CSV data and write cleaned Parquet.",
    )

    # ── 5. Glue Job 2 — Aggregate ─────────────────────────────────────────────

    job2_aggregate = GlueJobOperator(
        task_id        = "glue_job2_aggregate",
        job_name       = GLUE_JOB_AGGREGATE,
        script_args    = {
            **GLUE_BASE_ARGS,
            "--INPUT_PATH":  f"s3://{CLEANED_BUCKET}/cleaned/",
            "--OUTPUT_PATH": f"s3://{PROCESSED_BUCKET}/aggregated/",
        },
        aws_conn_id    = AWS_CONN_ID,
        region_name    = AWS_REGION,
        wait_for_completion = True,
        verbose        = True,
        doc_md         = "Compute KPI aggregations across region, category, product, customer, and time dimensions.",
    )

    # ── 6. Glue Job 3 — Consolidate → JSON ────────────────────────────────────

    job3_consolidate = GlueJobOperator(
        task_id        = "glue_job3_consolidate_json",
        job_name       = GLUE_JOB_CONSOLIDATE,
        script_args    = {
            **GLUE_BASE_ARGS,
            "--INPUT_PATH":  f"s3://{PROCESSED_BUCKET}/aggregated/",
            "--OUTPUT_PATH": f"s3://{OUTPUT_BUCKET}/consolidated/",
        },
        aws_conn_id    = AWS_CONN_ID,
        region_name    = AWS_REGION,
        wait_for_completion = True,
        verbose        = True,
        doc_md         = "Consolidate all aggregated datasets into a single structured JSON file.",
    )

    # ── 7. Verify output JSON exists ──────────────────────────────────────────

    def verify_output(**context):
        """Check that at least one JSON file was written to the output bucket."""
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        run_date = context["ds"].replace("-", "/")   # e.g. 2024/04/15
        prefix   = f"consolidated/{run_date}/"

        keys = s3.list_keys(bucket_name=OUTPUT_BUCKET, prefix=prefix)
        json_files = [k for k in (keys or []) if k.endswith(".json") and "etl_report" in k]

        if not json_files:
            raise ValueError(f"No consolidated JSON found at s3://{OUTPUT_BUCKET}/{prefix}")

        logger.info(f"Output verified: {json_files}")
        context["task_instance"].xcom_push(key="output_key", value=json_files[0])
        return json_files[0]

    verify_output_task = PythonOperator(
        task_id         = "verify_output_json",
        python_callable = verify_output,
        doc_md          = "Verify the consolidated JSON file was successfully written to S3.",
    )

    # ── 8. SNS notification on success ────────────────────────────────────────

    def build_success_message(**context):
        output_key = context["task_instance"].xcom_pull(
            task_ids="verify_output_json", key="output_key"
        )
        return json.dumps({
            "status":    "SUCCESS",
            "dag_id":    context["dag"].dag_id,
            "run_id":    context["run_id"],
            "exec_date": context["ds"],
            "output":    f"s3://{OUTPUT_BUCKET}/{output_key}",
        }, indent=2)

    build_msg = PythonOperator(
        task_id         = "build_success_message",
        python_callable = build_success_message,
        do_xcom_push    = True,
    )

    notify_success = SnsPublishOperator(
        task_id       = "notify_success",
        target_arn    = SNS_TOPIC_ARN,
        message       = "{{ task_instance.xcom_pull('build_success_message') }}",
        subject       = f"[ETL-POC] Pipeline SUCCESS — {{{{ ds }}}}",
        aws_conn_id   = AWS_CONN_ID,
        doc_md        = "Publish a success notification to SNS.",
    )

    # ── 9. SNS notification on failure ────────────────────────────────────────

    notify_failure = SnsPublishOperator(
        task_id       = "notify_failure",
        target_arn    = SNS_TOPIC_ARN,
        message       = json.dumps({
            "status":  "FAILED",
            "dag_id":  f"etl_pipeline_{ENV}",
            "exec_date": "{{ ds }}",
            "run_id":  "{{ run_id }}",
        }),
        subject       = f"[ETL-POC] Pipeline FAILED — {{{{ ds }}}}",
        aws_conn_id   = AWS_CONN_ID,
        trigger_rule  = TriggerRule.ONE_FAILED,
        doc_md        = "Publish a failure notification to SNS if any upstream task fails.",
    )

    # ── 10. End marker ────────────────────────────────────────────────────────

    end = EmptyOperator(
        task_id      = "end",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    #
    #  start
    #    └─► s3_sensor
    #          └─► run_crawler
    #                └─► job1_clean
    #                      └─► job2_aggregate
    #                            └─► job3_consolidate
    #                                  └─► verify_output
    #                                        └─► build_msg ──► notify_success ──► end
    #                                                                          ↗
    #                                              notify_failure ─────────────

    (
        start
        >> s3_sensor
        >> run_crawler
        >> job1_clean
        >> job2_aggregate
        >> job3_consolidate
        >> verify_output_task
        >> build_msg
        >> notify_success
        >> end
    )

    # Failure path — triggers if ANY task fails
    [s3_sensor, run_crawler, job1_clean, job2_aggregate, job3_consolidate,
     verify_output_task] >> notify_failure >> end
