###############################################################################
# variables.tf
###############################################################################

variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "project_name" {
  description = "Project name used as prefix for all resources"
  type        = string
  default     = "etl-poc"
}

variable "glue_workers" {
  description = "Number of Glue DPU workers per job"
  type        = number
  default     = 10

  validation {
    condition     = var.glue_workers >= 2 && var.glue_workers <= 100
    error_message = "glue_workers must be between 2 and 100."
  }
}

variable "glue_worker_type" {
  description = "Glue worker type (Standard, G.1X, G.2X, G.4X, G.8X)"
  type        = string
  default     = "G.1X"

  validation {
    condition     = contains(["Standard", "G.1X", "G.2X", "G.4X", "G.8X"], var.glue_worker_type)
    error_message = "Invalid glue_worker_type."
  }
}

variable "mwaa_environment_class" {
  description = "MWAA environment class (mw1.small, mw1.medium, mw1.large)"
  type        = string
  default     = "mw1.small"
}

variable "mwaa_max_workers" {
  description = "Maximum number of MWAA workers"
  type        = number
  default     = 5
}
