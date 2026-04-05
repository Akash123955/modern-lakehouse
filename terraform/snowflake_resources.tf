# =============================================================================
# Snowflake Resources — NYC Taxi Lakehouse
# =============================================================================

locals {
  db_name = upper(var.project_name)
  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ---------------------------------------------------------------------------
# 1. Warehouse
# ---------------------------------------------------------------------------
resource "snowflake_warehouse" "pipeline_wh" {
  name                         = upper("${var.project_name}_wh")
  comment                      = "Warehouse for ${var.project_name} pipelines (dbt, Airflow, Snowpipe)"
  warehouse_size               = var.warehouse_size
  auto_suspend                 = var.warehouse_auto_suspend_seconds
  auto_resume                  = var.warehouse_auto_resume
  initially_suspended          = true
  statement_timeout_in_seconds = 3600   # 1 hour max query time
  max_cluster_count            = 1      # Scale up in prod
  min_cluster_count            = 1
  scaling_policy               = "ECONOMY"
  enable_query_acceleration    = false  # Enable for analytical workloads
}

# ---------------------------------------------------------------------------
# 2. Database
# ---------------------------------------------------------------------------
resource "snowflake_database" "lakehouse_db" {
  name                        = local.db_name
  comment                     = "NYC Taxi modern lakehouse — medallion architecture (Bronze/Silver/Gold)"
  data_retention_time_in_days = var.environment == "prod" ? 14 : 1
}

# ---------------------------------------------------------------------------
# 3. Schemas (Medallion Layers)
# ---------------------------------------------------------------------------
resource "snowflake_schema" "raw_data" {
  database = snowflake_database.lakehouse_db.name
  name     = "RAW_DATA"
  comment  = "Raw ingestion zone: parquet COPY INTO landing and Snowpipe streaming table"
  data_retention_time_in_days = 7
}

resource "snowflake_schema" "bronze" {
  database = snowflake_database.lakehouse_db.name
  name     = "BRONZE"
  comment  = "Bronze layer: raw data + metadata columns (_loaded_at, _source, _file_name)"
  data_retention_time_in_days = 7
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.lakehouse_db.name
  name     = "SILVER"
  comment  = "Silver layer: cleaned, typed, validated, and enriched records"
  data_retention_time_in_days = 14
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.lakehouse_db.name
  name     = "GOLD"
  comment  = "Gold layer: analytics-ready aggregates for BI and ML consumption"
  data_retention_time_in_days = 30
}

resource "snowflake_schema" "snapshots" {
  database = snowflake_database.lakehouse_db.name
  name     = "SNAPSHOTS"
  comment  = "dbt SCD Type 2 snapshots — historical reference data"
  data_retention_time_in_days = 30
}

# ---------------------------------------------------------------------------
# 4. Roles (RBAC)
# ---------------------------------------------------------------------------

# Pipeline role: full access for dbt, Airflow, Snowpipe
resource "snowflake_role" "pipeline_role" {
  provider = snowflake.security_admin
  name     = upper("${var.project_name}_pipeline_role")
  comment  = "Full pipeline access: dbt transforms, Airflow orchestration, Snowpipe ingest"
}

# Analyst role: read-only access to silver + gold
resource "snowflake_role" "analyst_role" {
  provider = snowflake.security_admin
  name     = upper("${var.project_name}_analyst_role")
  comment  = "Read-only access to silver and gold schemas for BI and analytics"
}

# Readonly role: read-only access to gold only (for external dashboards)
resource "snowflake_role" "readonly_role" {
  provider = snowflake.security_admin
  name     = upper("${var.project_name}_readonly_role")
  comment  = "Read-only access to gold layer for external BI tools (Tableau, Metabase)"
}

# ---------------------------------------------------------------------------
# 5. Service Account User
# ---------------------------------------------------------------------------
resource "snowflake_user" "service_account" {
  provider     = snowflake.security_admin
  name         = upper(var.service_account_user)
  password     = var.service_account_password
  comment      = "Service account for ${var.project_name} pipelines"
  email        = var.service_account_email
  display_name = "${var.project_name} Pipeline Service Account"

  default_warehouse = snowflake_warehouse.pipeline_wh.name
  default_role      = snowflake_role.pipeline_role.name
  default_namespace = "${snowflake_database.lakehouse_db.name}.RAW_DATA"

  must_change_password = false
  disabled             = false
}

# Assign pipeline role to service account
resource "snowflake_role_grants" "service_account_pipeline_role" {
  provider  = snowflake.security_admin
  role_name = snowflake_role.pipeline_role.name
  users     = [snowflake_user.service_account.name]
}

# ---------------------------------------------------------------------------
# 6. Warehouse Grants
# ---------------------------------------------------------------------------
resource "snowflake_warehouse_grant" "pipeline_wh_usage" {
  warehouse_name = snowflake_warehouse.pipeline_wh.name
  privilege      = "USAGE"
  roles          = [snowflake_role.pipeline_role.name]
  with_grant_option = false
}

resource "snowflake_warehouse_grant" "analyst_wh_usage" {
  warehouse_name = snowflake_warehouse.pipeline_wh.name
  privilege      = "USAGE"
  roles          = [snowflake_role.analyst_role.name]
  with_grant_option = false
}

resource "snowflake_warehouse_grant" "readonly_wh_usage" {
  warehouse_name = snowflake_warehouse.pipeline_wh.name
  privilege      = "USAGE"
  roles          = [snowflake_role.readonly_role.name]
  with_grant_option = false
}

# ---------------------------------------------------------------------------
# 7. Database Grants
# ---------------------------------------------------------------------------
resource "snowflake_database_grant" "pipeline_db_usage" {
  database_name = snowflake_database.lakehouse_db.name
  privilege     = "USAGE"
  roles         = [snowflake_role.pipeline_role.name, snowflake_role.analyst_role.name, snowflake_role.readonly_role.name]
}

# ---------------------------------------------------------------------------
# 8. Schema Grants — Granular RBAC per Layer
# ---------------------------------------------------------------------------

# Pipeline role: full access to all schemas
resource "snowflake_schema_grant" "pipeline_raw_data" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.raw_data.name
  privilege     = "ALL PRIVILEGES"
  roles         = [snowflake_role.pipeline_role.name]
}

resource "snowflake_schema_grant" "pipeline_bronze" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.bronze.name
  privilege     = "ALL PRIVILEGES"
  roles         = [snowflake_role.pipeline_role.name]
}

resource "snowflake_schema_grant" "pipeline_silver" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.silver.name
  privilege     = "ALL PRIVILEGES"
  roles         = [snowflake_role.pipeline_role.name]
}

resource "snowflake_schema_grant" "pipeline_gold" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.gold.name
  privilege     = "ALL PRIVILEGES"
  roles         = [snowflake_role.pipeline_role.name]
}

resource "snowflake_schema_grant" "pipeline_snapshots" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.snapshots.name
  privilege     = "ALL PRIVILEGES"
  roles         = [snowflake_role.pipeline_role.name]
}

# Analyst role: read silver + gold + snapshots (no raw/bronze)
resource "snowflake_schema_grant" "analyst_silver" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.silver.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst_role.name]
}

resource "snowflake_schema_grant" "analyst_gold" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.gold.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst_role.name]
}

resource "snowflake_schema_grant" "analyst_snapshots" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.snapshots.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst_role.name]
}

# Readonly role: gold only
resource "snowflake_schema_grant" "readonly_gold" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.gold.name
  privilege     = "USAGE"
  roles         = [snowflake_role.readonly_role.name]
}

# ---------------------------------------------------------------------------
# 9. Table Grants — Future grants for analyst + readonly
# ---------------------------------------------------------------------------
resource "snowflake_table_grant" "analyst_silver_select" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.silver.name
  privilege     = "SELECT"
  roles         = [snowflake_role.analyst_role.name]
  on_future     = true
}

resource "snowflake_table_grant" "analyst_gold_select" {
  database_name = snowflake_database.lakehouse_db.name
  schema_name   = snowflake_schema.gold.name
  privilege     = "SELECT"
  roles         = [snowflake_role.analyst_role.name, snowflake_role.readonly_role.name]
  on_future     = true
}
