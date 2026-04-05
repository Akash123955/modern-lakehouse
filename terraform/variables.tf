# =============================================================================
# Terraform Variables — NYC Taxi Lakehouse on Snowflake
# =============================================================================
# Pass sensitive values via environment variables or a tfvars file.
# Never hardcode credentials in this file.
#
# Usage:
#   export TF_VAR_snowflake_account="your_account"
#   export TF_VAR_snowflake_username="your_user"
#   export TF_VAR_snowflake_password="your_password"
#   terraform apply
# =============================================================================

variable "snowflake_account" {
  description = "Snowflake account identifier (e.g., abc12345.us-east-1)"
  type        = string
  sensitive   = false
}

variable "snowflake_username" {
  description = "Snowflake admin user for Terraform (must have SYSADMIN + SECURITYADMIN)"
  type        = string
  sensitive   = false
}

variable "snowflake_password" {
  description = "Snowflake admin password"
  type        = string
  sensitive   = true
}

variable "snowflake_region" {
  description = "Snowflake region (e.g., us-east-1, eu-west-1)"
  type        = string
  default     = "us-east-1"
}

# ---------------------------------------------------------------------------
# Project settings
# ---------------------------------------------------------------------------
variable "project_name" {
  description = "Project identifier used in resource names and tags"
  type        = string
  default     = "nyc_taxi_lakehouse"
}

variable "environment" {
  description = "Deployment environment: dev, staging, or prod"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod"
  }
}

# ---------------------------------------------------------------------------
# Warehouse settings
# ---------------------------------------------------------------------------
variable "warehouse_size" {
  description = "Snowflake warehouse size for the pipeline warehouse"
  type        = string
  default     = "X-SMALL"

  validation {
    condition = contains(
      ["X-SMALL", "SMALL", "MEDIUM", "LARGE", "X-LARGE"],
      var.warehouse_size
    )
    error_message = "warehouse_size must be a valid Snowflake warehouse size"
  }
}

variable "warehouse_auto_suspend_seconds" {
  description = "Seconds of inactivity before auto-suspend (cost control)"
  type        = number
  default     = 300  # 5 minutes
}

variable "warehouse_auto_resume" {
  description = "Automatically resume warehouse on query"
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Service account settings
# ---------------------------------------------------------------------------
variable "service_account_user" {
  description = "Snowflake service account user for pipelines (dbt, Airflow, Snowpipe)"
  type        = string
  default     = "nyc_taxi_user"
}

variable "service_account_password" {
  description = "Password for the pipeline service account"
  type        = string
  sensitive   = true
}

variable "service_account_email" {
  description = "Email for the service account (for notifications)"
  type        = string
  default     = "data-engineering@example.com"
}
