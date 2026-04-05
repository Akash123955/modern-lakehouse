# =============================================================================
# Terraform Main — NYC Taxi Lakehouse Infrastructure
# =============================================================================
# Provisions all Snowflake infrastructure for the modern lakehouse.
# Replaces the manual snowflake_setup.sql with version-controlled,
# reproducible, auditable IaC.
#
# Resources managed:
#   - Snowflake warehouse (with auto-suspend)
#   - Database + 5 schemas (raw_data, bronze, silver, gold, snapshots)
#   - Roles (admin, pipeline, analyst, readonly)
#   - Service account user
#   - RBAC grants per schema
#
# Prerequisites:
#   terraform init
#   export TF_VAR_snowflake_account=...
#   export TF_VAR_snowflake_username=...
#   export TF_VAR_snowflake_password=...
#   export TF_VAR_service_account_password=...
# =============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.89"
    }
  }

  # Remote state — uncomment for team use (recommended in production)
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "nyc-taxi-lakehouse/terraform.tfstate"
  #   region         = "us-east-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-locks"
  # }
}

provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_username
  password = var.snowflake_password
  region   = var.snowflake_region
  role     = "SYSADMIN"
}

# Secondary provider with SECURITYADMIN for user/role management
provider "snowflake" {
  alias    = "security_admin"
  account  = var.snowflake_account
  username = var.snowflake_username
  password = var.snowflake_password
  region   = var.snowflake_region
  role     = "SECURITYADMIN"
}

# Secondary provider with ACCOUNTADMIN for account-level grants
provider "snowflake" {
  alias    = "account_admin"
  account  = var.snowflake_account
  username = var.snowflake_username
  password = var.snowflake_password
  region   = var.snowflake_region
  role     = "ACCOUNTADMIN"
}
