###############################################################################
# outputs.tf
###############################################################################

output "raw_bucket_name" {
  description = "S3 bucket for raw CSV input"
  value       = aws_s3_bucket.raw.bucket
}

output "cleaned_bucket_name" {
  description = "S3 bucket for cleaned/validated data"
  value       = aws_s3_bucket.cleaned.bucket
}

output "processed_bucket_name" {
  description = "S3 bucket for aggregated data"
  value       = aws_s3_bucket.processed.bucket
}

output "output_bucket_name" {
  description = "S3 bucket for final consolidated JSON"
  value       = aws_s3_bucket.output.bucket
}

output "glue_scripts_bucket" {
  description = "S3 bucket holding Glue PySpark scripts"
  value       = aws_s3_bucket.glue_scripts.bucket
}

output "airflow_bucket_name" {
  description = "S3 bucket for MWAA DAGs and requirements"
  value       = aws_s3_bucket.airflow.bucket
}

output "glue_job_clean" {
  description = "Glue Job 1 — Clean & Validate"
  value       = aws_glue_job.clean.name
}

output "glue_job_aggregate" {
  description = "Glue Job 2 — Aggregate & Transform"
  value       = aws_glue_job.aggregate.name
}

output "glue_job_consolidate" {
  description = "Glue Job 3 — Consolidate to JSON"
  value       = aws_glue_job.consolidate.name
}

output "glue_catalog_database" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.etl_db.name
}

output "mwaa_environment_name" {
  description = "MWAA Airflow environment name"
  value       = aws_mwaa_environment.etl.name
}

output "mwaa_webserver_url" {
  description = "Airflow UI URL"
  value       = aws_mwaa_environment.etl.webserver_url
}

output "glue_role_arn" {
  description = "IAM Role ARN for Glue jobs"
  value       = aws_iam_role.glue.arn
}

output "mwaa_role_arn" {
  description = "IAM Role ARN for MWAA"
  value       = aws_iam_role.mwaa.arn
}

output "upload_csv_command" {
  description = "Sample AWS CLI command to upload a test CSV"
  value       = "aws s3 cp ./data_generator/sample_sales.csv s3://${aws_s3_bucket.raw.bucket}/input/sales/dt=$(date +%Y-%m-%d)/"
}
