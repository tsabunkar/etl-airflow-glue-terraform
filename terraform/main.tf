###############################################################################
# ETL POC — Airflow + Glue + S3  (Terraform)
# Architecture:
#   S3 (raw CSV) → Glue Job 1 (clean/validate) → S3 (cleaned)
#                → Glue Job 2 (aggregate/transform) → S3 (processed)
#                → Glue Job 3 (consolidate → JSON) → S3 (output)
#   Orchestrated by Apache Airflow on MWAA
###############################################################################

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "etl-poc"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

###############################################################################
# DATA SOURCES
###############################################################################
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  region      = data.aws_region.current.name
  name_prefix = "${var.project_name}-${var.environment}"
}

###############################################################################
# S3 BUCKETS
###############################################################################

# --- Raw input bucket (huge CSV files land here) ---
resource "aws_s3_bucket" "raw" {
  bucket        = "${local.name_prefix}-raw-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Cleaned / intermediate bucket ---
resource "aws_s3_bucket" "cleaned" {
  bucket        = "${local.name_prefix}-cleaned-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "cleaned" {
  bucket = aws_s3_bucket.cleaned.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

resource "aws_s3_bucket_public_access_block" "cleaned" {
  bucket                  = aws_s3_bucket.cleaned.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Processed / aggregated bucket ---
resource "aws_s3_bucket" "processed" {
  bucket        = "${local.name_prefix}-processed-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Final consolidated JSON output ---
resource "aws_s3_bucket" "output" {
  bucket        = "${local.name_prefix}-output-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "output" {
  bucket = aws_s3_bucket.output.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

resource "aws_s3_bucket_public_access_block" "output" {
  bucket                  = aws_s3_bucket.output.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Glue scripts bucket ---
resource "aws_s3_bucket" "glue_scripts" {
  bucket        = "${local.name_prefix}-glue-scripts-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket                  = aws_s3_bucket.glue_scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- MWAA (Airflow) bucket ---
resource "aws_s3_bucket" "airflow" {
  bucket        = "${local.name_prefix}-airflow-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "airflow" {
  bucket = aws_s3_bucket.airflow.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_public_access_block" "airflow" {
  bucket                  = aws_s3_bucket.airflow.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

###############################################################################
# UPLOAD GLUE SCRIPTS TO S3
###############################################################################

resource "aws_s3_object" "glue_clean" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/glue_job1_clean.py"
  source = "${path.module}/../glue_scripts/glue_job1_clean.py"
  etag   = filemd5("${path.module}/../glue_scripts/glue_job1_clean.py")
}

resource "aws_s3_object" "glue_aggregate" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/glue_job2_aggregate.py"
  source = "${path.module}/../glue_scripts/glue_job2_aggregate.py"
  etag   = filemd5("${path.module}/../glue_scripts/glue_job2_aggregate.py")
}

resource "aws_s3_object" "glue_consolidate" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/glue_job3_consolidate.py"
  source = "${path.module}/../glue_scripts/glue_job3_consolidate.py"
  etag   = filemd5("${path.module}/../glue_scripts/glue_job3_consolidate.py")
}

resource "aws_s3_object" "airflow_dag" {
  bucket = aws_s3_bucket.airflow.id
  key    = "dags/etl_pipeline_dag.py"
  source = "${path.module}/../airflow_dags/etl_pipeline_dag.py"
  etag   = filemd5("${path.module}/../airflow_dags/etl_pipeline_dag.py")
}

###############################################################################
# IAM — GLUE ROLE
###############################################################################

resource "aws_iam_role" "glue" {
  name = "${local.name_prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.name_prefix}-glue-s3-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*",
          aws_s3_bucket.cleaned.arn, "${aws_s3_bucket.cleaned.arn}/*",
          aws_s3_bucket.processed.arn, "${aws_s3_bucket.processed.arn}/*",
          aws_s3_bucket.output.arn, "${aws_s3_bucket.output.arn}/*",
          aws_s3_bucket.glue_scripts.arn, "${aws_s3_bucket.glue_scripts.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:/aws-glue/*"
      }
    ]
  })
}

###############################################################################
# IAM — MWAA ROLE
###############################################################################

resource "aws_iam_role" "mwaa" {
  name = "${local.name_prefix}-mwaa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "airflow-env.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "mwaa_policy" {
  name = "${local.name_prefix}-mwaa-policy"
  role = aws_iam_role.mwaa.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject*", "s3:GetBucket*", "s3:List*",
                  "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          aws_s3_bucket.airflow.arn,
          "${aws_s3_bucket.airflow.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns",
                    "glue:BatchStopJobRun", "glue:GetJob"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream",
                    "logs:PutLogEvents", "logs:GetLogEvents",
                    "logs:GetLogRecord", "logs:GetQueryResults",
                    "logs:DescribeLogGroups"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:ChangeMessageVisibility", "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes", "sqs:GetQueueUrl",
                    "sqs:ReceiveMessage", "sqs:SendMessage"]
        Resource = "arn:aws:sqs:${local.region}:*:airflow-celery-*"
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*", "kms:Encrypt"]
        Resource = "*"
      }
    ]
  })
}

###############################################################################
# GLUE DATABASE & CRAWLER
###############################################################################

resource "aws_glue_catalog_database" "etl_db" {
  name        = "${replace(local.name_prefix, "-", "_")}_db"
  description = "ETL POC Glue Data Catalog Database"
}

resource "aws_glue_crawler" "raw_csv" {
  name          = "${local.name_prefix}-raw-crawler"
  database_name = aws_glue_catalog_database.etl_db.name
  role          = aws_iam_role.glue.arn
  description   = "Crawls raw CSV files in S3 and infers schema"

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/input/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  schedule = "cron(0 1 * * ? *)" # daily at 01:00 UTC
}

###############################################################################
# GLUE JOBS
###############################################################################

# Shared job defaults
locals {
  glue_default_args = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.glue_scripts.bucket}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_scripts.bucket}/temp/"
    "--RAW_BUCKET"                       = aws_s3_bucket.raw.bucket
    "--CLEANED_BUCKET"                   = aws_s3_bucket.cleaned.bucket
    "--PROCESSED_BUCKET"                 = aws_s3_bucket.processed.bucket
    "--OUTPUT_BUCKET"                    = aws_s3_bucket.output.bucket
  }
}

# Job 1 — Clean & Validate
resource "aws_glue_job" "clean" {
  name              = "${local.name_prefix}-job1-clean"
  role_arn          = aws_iam_role.glue.arn
  description       = "Job 1: Read raw CSVs, clean, validate, deduplicate"
  glue_version      = "4.0"
  number_of_workers = var.glue_workers
  worker_type       = var.glue_worker_type
  timeout           = 120

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/glue_job1_clean.py"
    python_version  = "3"
  }

  default_arguments = merge(local.glue_default_args, {
    "--INPUT_PATH"  = "s3://${aws_s3_bucket.raw.bucket}/input/"
    "--OUTPUT_PATH" = "s3://${aws_s3_bucket.cleaned.bucket}/cleaned/"
  })

  execution_property { max_concurrent_runs = 3 }

  tags = { Stage = "1-clean" }
}

# Job 2 — Aggregate & Transform
resource "aws_glue_job" "aggregate" {
  name              = "${local.name_prefix}-job2-aggregate"
  role_arn          = aws_iam_role.glue.arn
  description       = "Job 2: Aggregate cleaned data, compute KPIs"
  glue_version      = "4.0"
  number_of_workers = var.glue_workers
  worker_type       = var.glue_worker_type
  timeout           = 120

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/glue_job2_aggregate.py"
    python_version  = "3"
  }

  default_arguments = merge(local.glue_default_args, {
    "--INPUT_PATH"  = "s3://${aws_s3_bucket.cleaned.bucket}/cleaned/"
    "--OUTPUT_PATH" = "s3://${aws_s3_bucket.processed.bucket}/aggregated/"
  })

  execution_property { max_concurrent_runs = 3 }

  tags = { Stage = "2-aggregate" }
}

# Job 3 — Consolidate → JSON
resource "aws_glue_job" "consolidate" {
  name              = "${local.name_prefix}-job3-consolidate"
  role_arn          = aws_iam_role.glue.arn
  description       = "Job 3: Consolidate all aggregated data into single JSON file"
  glue_version      = "4.0"
  number_of_workers = var.glue_workers
  worker_type       = var.glue_worker_type
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/glue_job3_consolidate.py"
    python_version  = "3"
  }

  default_arguments = merge(local.glue_default_args, {
    "--INPUT_PATH"  = "s3://${aws_s3_bucket.processed.bucket}/aggregated/"
    "--OUTPUT_PATH" = "s3://${aws_s3_bucket.output.bucket}/consolidated/"
  })

  execution_property { max_concurrent_runs = 1 }

  tags = { Stage = "3-consolidate" }
}

###############################################################################
# GLUE TRIGGERS (fallback — Airflow is the primary orchestrator)
###############################################################################

resource "aws_glue_trigger" "crawler_done" {
  name     = "${local.name_prefix}-crawler-trigger"
  type     = "CONDITIONAL"
  enabled  = false   # Airflow controls scheduling; enable for standalone mode

  actions {
    job_name = aws_glue_job.clean.name
  }

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.raw_csv.name
      crawl_state  = "SUCCEEDED"
    }
  }
}

###############################################################################
# CLOUDWATCH LOG GROUPS
###############################################################################

resource "aws_cloudwatch_log_group" "glue_clean" {
  name              = "/aws-glue/jobs/${aws_glue_job.clean.name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "glue_aggregate" {
  name              = "/aws-glue/jobs/${aws_glue_job.aggregate.name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "glue_consolidate" {
  name              = "/aws-glue/jobs/${aws_glue_job.consolidate.name}"
  retention_in_days = 14
}

###############################################################################
# VPC FOR MWAA
###############################################################################

resource "aws_vpc" "mwaa" {
  cidr_block           = "10.192.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = { Name = "${local.name_prefix}-mwaa-vpc" }
}

resource "aws_internet_gateway" "mwaa" {
  vpc_id = aws_vpc.mwaa.id
  tags   = { Name = "${local.name_prefix}-igw" }
}

resource "aws_subnet" "public_a" {
  vpc_id                  = aws_vpc.mwaa.id
  cidr_block              = "10.192.10.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true
  tags                    = { Name = "${local.name_prefix}-public-a" }
}

resource "aws_subnet" "public_b" {
  vpc_id                  = aws_vpc.mwaa.id
  cidr_block              = "10.192.11.0/24"
  availability_zone       = "${var.aws_region}b"
  map_public_ip_on_launch = true
  tags                    = { Name = "${local.name_prefix}-public-b" }
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.mwaa.id
  cidr_block        = "10.192.20.0/24"
  availability_zone = "${var.aws_region}a"
  tags              = { Name = "${local.name_prefix}-private-a" }
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.mwaa.id
  cidr_block        = "10.192.21.0/24"
  availability_zone = "${var.aws_region}b"
  tags              = { Name = "${local.name_prefix}-private-b" }
}

resource "aws_eip" "nat" { domain = "vpc" }

resource "aws_nat_gateway" "mwaa" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public_a.id
  tags          = { Name = "${local.name_prefix}-nat" }
  depends_on    = [aws_internet_gateway.mwaa]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.mwaa.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.mwaa.id
  }
  tags = { Name = "${local.name_prefix}-public-rt" }
}

resource "aws_route_table_association" "public_a" {
  subnet_id      = aws_subnet.public_a.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "public_b" {
  subnet_id      = aws_subnet.public_b.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.mwaa.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.mwaa.id
  }
  tags = { Name = "${local.name_prefix}-private-rt" }
}

resource "aws_route_table_association" "private_a" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_b" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.private.id
}

resource "aws_security_group" "mwaa" {
  name        = "${local.name_prefix}-mwaa-sg"
  description = "Security group for MWAA environment"
  vpc_id      = aws_vpc.mwaa.id

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

###############################################################################
# MWAA — Amazon Managed Workflows for Apache Airflow
###############################################################################

resource "aws_mwaa_environment" "etl" {
  name               = "${local.name_prefix}-airflow"
  airflow_version    = "2.8.1"
  environment_class  = var.mwaa_environment_class
  min_workers        = 1
  max_workers        = var.mwaa_max_workers
  execution_role_arn = aws_iam_role.mwaa.arn

  source_bucket_arn    = aws_s3_bucket.airflow.arn
  dag_s3_path          = "dags/"
  requirements_s3_path = "requirements.txt"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  airflow_configuration_options = {
    "core.default_timezone"   = "UTC"
    "core.dag_concurrency"    = "16"
    "core.max_active_runs_per_dag" = "5"
    "scheduler.dag_dir_list_interval" = "30"
  }

  tags = { Name = "${local.name_prefix}-mwaa" }

  depends_on = [
    aws_s3_object.airflow_dag,
    aws_iam_role_policy.mwaa_policy,
  ]
}

###############################################################################
# S3 LIFECYCLE — auto-expire intermediate data
###############################################################################

resource "aws_s3_bucket_lifecycle_configuration" "cleaned" {
  bucket = aws_s3_bucket.cleaned.id
  rule {
    id     = "expire-cleaned-after-30d"
    status = "Enabled"
    filter { prefix = "cleaned/" }
    expiration { days = 30 }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule {
    id     = "expire-processed-after-30d"
    status = "Enabled"
    filter { prefix = "aggregated/" }
    expiration { days = 30 }
  }
}
