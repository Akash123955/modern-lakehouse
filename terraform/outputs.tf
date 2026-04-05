# =============================================================================
# Terraform Outputs — NYC Taxi Lakehouse
# =============================================================================
# These outputs are used by other tooling (Airflow connections, dbt profiles,
# CI/CD pipelines) to reference provisioned infrastructure without hardcoding.
# =============================================================================

output "warehouse_name" {
  description = "Snowflake warehouse name for pipeline runs"
  value       = snowflake_warehouse.pipeline_wh.name
}

output "database_name" {
  description = "Snowflake database name"
  value       = snowflake_database.lakehouse_db.name
}

output "schemas" {
  description = "All provisioned schema names"
  value = {
    raw_data  = snowflake_schema.raw_data.name
    bronze    = snowflake_schema.bronze.name
    silver    = snowflake_schema.silver.name
    gold      = snowflake_schema.gold.name
    snapshots = snowflake_schema.snapshots.name
  }
}

output "roles" {
  description = "RBAC roles provisioned for this project"
  value = {
    pipeline = snowflake_role.pipeline_role.name
    analyst  = snowflake_role.analyst_role.name
    readonly = snowflake_role.readonly_role.name
  }
}

output "service_account_username" {
  description = "Service account username for pipelines"
  value       = snowflake_user.service_account.name
  sensitive   = false
}

output "dbt_profile_snippet" {
  description = "Ready-to-use dbt profiles.yml snippet for this environment"
  sensitive   = true
  value = <<-EOT
    nyc_taxi_lakehouse:
      target: ${var.environment}
      outputs:
        ${var.environment}:
          type: snowflake
          account: ${var.snowflake_account}
          user: ${upper(var.service_account_user)}
          password: <service_account_password>
          role: ${snowflake_role.pipeline_role.name}
          warehouse: ${snowflake_warehouse.pipeline_wh.name}
          database: ${snowflake_database.lakehouse_db.name}
          schema: RAW_DATA
          threads: ${var.environment == "prod" ? 8 : 4}
          query_tag: dbt_${var.environment}
  EOT
}
