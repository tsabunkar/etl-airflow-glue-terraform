###############################################################################
# terraform.tfvars  — example values (copy and edit for your environment)
###############################################################################

aws_region             = "us-east-1"
environment            = "dev"
project_name           = "etl-poc"

# Glue compute
glue_workers           = 10
glue_worker_type       = "G.1X"   # 4 vCPU, 16 GB RAM per worker

# MWAA / Airflow
mwaa_environment_class = "mw1.small"
mwaa_max_workers       = 5
