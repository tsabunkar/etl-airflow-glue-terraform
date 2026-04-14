# ETL POC — Airflow + Glue + S3

A production-grade ETL proof-of-concept using **Apache Airflow (MWAA)** and **AWS Glue PySpark Jobs** to process large CSV files into a consolidated JSON report.

---

## Architecture

```
                    ┌─────────────────────────────────────────────────────┐
                    │              MWAA (Apache Airflow)                  │
                    │                                                     │
                    │  S3 Sensor → Crawler → Job1 → Job2 → Job3 → SNS   │
                    └────────────────────────┬────────────────────────────┘
                                             │ orchestrates
          ┌──────────────────────────────────▼──────────────────────────────┐
          │                                                                  │
   ┌──────▼──────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐│
   │  S3 Raw     │    │  S3 Cleaned │    │ S3 Processed│    │  S3 Output  ││
   │  (CSV)      │───►│  (Parquet)  │───►│  (Parquet)  │───►│  (JSON)     ││
   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘│
         │                  │                  │                  │         │
   ┌─────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐│
   │ Glue       │    │  Glue Job 1 │    │  Glue Job 2 │    │  Glue Job 3 ││
   │ Crawler    │    │  Clean &    │    │  Aggregate  │    │  Consolidate││
   │            │    │  Validate   │    │  Transform  │    │  → JSON     ││
   └────────────┘    └─────────────┘    └─────────────┘    └─────────────┘│
                                                                            │
   └──────────────────────── AWS Glue ─────────────────────────────────────┘
```

---

## Project Structure

```
etl-poc/
├── terraform/
│   ├── main.tf              # All AWS infrastructure (S3, Glue, MWAA, IAM, VPC)
│   ├── variables.tf         # Input variables
│   ├── outputs.tf           # Output values
│   └── terraform.tfvars     # Example variable values
│
├── glue_scripts/
│   ├── glue_job1_clean.py       # PySpark: read CSV → clean → Parquet
│   ├── glue_job2_aggregate.py   # PySpark: Parquet → 8 KPI aggregations
│   └── glue_job3_consolidate.py # PySpark: all aggregations → 1 JSON
│
├── airflow_dags/
│   ├── etl_pipeline_dag.py  # Airflow DAG orchestrating the full pipeline
│   └── requirements.txt     # MWAA pip requirements
│
└── data_generator/
    └── generate_sample_data.py  # Generates 1M+ row test CSV
```

---

## Pipeline Stages

### Stage 1 — Glue Job 1: Clean & Validate

- Reads raw CSVs with enforced schema (no full-scan type inference)
- Trims whitespace, replaces empty strings with null
- Validates required fields; quarantines bad rows
- Casts types (dates with multiple format support, numerics)
- Business rule validation (sales_amount reconciliation)
- Deduplicates on `(order_id, product_id)`
- Writes partitioned Parquet `year=YYYY/month=MM/`

### Stage 2 — Glue Job 2: Aggregate & Transform

Produces **8 aggregated datasets**:
| Dataset | Description |
|---|---|
| `region_country_monthly` | Revenue/profit by region, country, month |
| `category_monthly` | Revenue/profit by product category |
| `monthly_trend` | MoM revenue trends with growth % |
| `top_products_by_region` | Top 10 products per region |
| `customer_segments` | HIGH/MID/LOW value + ACTIVE/AT_RISK/CHURNED |
| `payment_distribution` | Revenue by payment method per region |
| `order_status_summary` | Order counts by status per region |
| `overall_kpi` | Single-row global KPI summary |

### Stage 3 — Glue Job 3: Consolidate → JSON

- Reads all 8 Parquet datasets from Stage 2
- Builds a nested JSON document with all sections
- Writes a single `etl_report_<run_id>.json` + `manifest_<run_id>.json` to S3
- JSON is BI-ready and can be consumed by QuickSight, dashboards, or APIs

---

## Prerequisites

| Tool         | Version                     |
| ------------ | --------------------------- |
| Terraform    | >= 1.5.0                    |
| AWS CLI      | >= 2.0                      |
| Python       | >= 3.9 (for data generator) |
| AWS Provider | ~> 5.0                      |

---

## Deployment

### Step 1 — Configure AWS credentials

```bash
aws configure
# or use AWS_PROFILE / IAM role
```

### Step 2 — Initialize Terraform

```bash
cd terraform/
terraform init
```

### Step 3 — Review the plan

```bash
terraform plan -var-file=terraform.tfvars
```

### Step 4 — Deploy infrastructure (~15-20 min for MWAA)

```bash
terraform apply -var-file=terraform.tfvars
```

### Step 5 — Upload Airflow requirements.txt

```bash
AIRFLOW_BUCKET=$(terraform output -raw airflow_bucket_name)
aws s3 cp ../airflow_dags/requirements.txt s3://$AIRFLOW_BUCKET/requirements.txt
```

### Step 6 — Set Airflow Variables (in MWAA UI or CLI)

```bash
MWAA_ENV=$(terraform output -raw mwaa_environment_name)

# Set all required variables
for VAR in \
  "environment=dev" \
  "project_name=etl-poc" \
  "aws_region=us-east-1" \
  "aws_conn_id=aws_default" \
  "raw_bucket=$(terraform output -raw raw_bucket_name)" \
  "cleaned_bucket=$(terraform output -raw cleaned_bucket_name)" \
  "processed_bucket=$(terraform output -raw processed_bucket_name)" \
  "output_bucket=$(terraform output -raw output_bucket_name)" \
  "glue_scripts_bucket=$(terraform output -raw glue_scripts_bucket)" \
  "glue_role_arn=$(terraform output -raw glue_role_arn)" \
  "sns_topic_arn=arn:aws:sns:us-east-1:ACCOUNT_ID:etl-alerts"
do
  KEY="${VAR%%=*}"
  VAL="${VAR#*=}"
  aws mwaa create-cli-token --name $MWAA_ENV | \
    xargs -I{} curl -s -X POST \
      "https://<mwaa-webserver>/api/v1/variables" \
      -H "Authorization: Bearer {}" \
      -d "{\"key\": \"$KEY\", \"value\": \"$VAL\"}"
done
```

### Step 7 — Generate & upload test data

```bash
cd ../data_generator/
pip install -r requirements.txt

# Generate 1M rows (~150 MB CSV)
python generate_sample_data.py \
  --rows 1000000 \
  --output sample_sales.csv \
  --upload \
  --bucket $(cd ../terraform && terraform output -raw raw_bucket_name)
```

### Step 8 — Trigger the DAG

```bash
# Via Airflow UI at the MWAA webserver URL, or:
AIRFLOW_URL=$(cd terraform && terraform output -raw mwaa_webserver_url)
echo "Open Airflow UI: https://$AIRFLOW_URL"
# Enable and trigger: etl_pipeline_dev
```

---

## Output JSON Structure

```json
{
  "metadata": {
    "run_id": "uuid",
    "generated_at": "2024-04-15T02:30:00Z",
    "data_range": { "from": "2021-01-01", "to": "2024-12-31" },
    "record_counts": { ... },
    "customer_segment_totals": { "HIGH_VALUE": 1234, ... }
  },
  "overall_kpi": {
    "grand_total_revenue": 12345678.90,
    "grand_total_profit": 3456789.12,
    "total_orders": 98765,
    "unique_customers": 45321,
    "avg_order_value": 125.00
  },
  "regional_summary": [ { "region": "...", "total_revenue": ... } ],
  "category_summary": [ { "category": "...", "sub_category": "...", ... } ],
  "monthly_trends": [ { "year": 2024, "month": 1, "region": "...", ... } ],
  "top_products": { "North America": [ { "rank_in_region": 1, ... } ] },
  "customer_segments": [ { "customer_segment": "HIGH_VALUE", ... } ],
  "payment_distribution": [ { "payment_method": "Credit Card", ... } ],
  "order_status_summary": [ { "status": "Delivered", ... } ]
}
```

---

## Cost Estimate (dev environment, daily runs)

| Service     | Config                         | Estimated Cost/Month |
| ----------- | ------------------------------ | -------------------- |
| MWAA        | mw1.small                      | ~$80                 |
| Glue Jobs   | 10 x G.1X workers, ~30 min/day | ~$15                 |
| S3 Storage  | ~500 GB across all buckets     | ~$12                 |
| NAT Gateway | Low traffic                    | ~$35                 |
| **Total**   |                                | **~$142/month**      |

---

## Cleanup

```bash
cd terraform/
terraform destroy -var-file=terraform.tfvars
```

---

## Customization

| What to change        | Where                                            |
| --------------------- | ------------------------------------------------ |
| Input CSV schema      | `glue_job1_clean.py` → `RAW_SCHEMA`              |
| Add new aggregation   | `glue_job2_aggregate.py` → add new `agg_*` block |
| Change JSON structure | `glue_job3_consolidate.py` → `consolidated` dict |
| DAG schedule          | `etl_pipeline_dag.py` → `schedule_interval`      |
| Scale Glue workers    | `terraform.tfvars` → `glue_workers`              |

---

## AWS Components created

- 5 S3 buckets — raw, cleaned, processed, output, glue-scripts
- 3 AWS Glue Jobs (Glue 4.0, G.1X workers)
- 1 Glue Crawler with daily schedule
- MWAA (Airflow 2.8.1) with VPC, NAT Gateway, private subnets
- IAM roles for Glue and MWAA with least-privilege policies
- CloudWatch Log Groups, S3 lifecycle rules
