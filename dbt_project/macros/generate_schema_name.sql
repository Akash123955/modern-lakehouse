/*
    Macro: generate_schema_name
    Description: Overrides the default dbt schema naming behavior to use the
                 custom_schema_name exactly as provided (without prepending the
                 target schema as a prefix). This ensures that models configured
                 with +schema: bronze land in BRONZE rather than <target>_BRONZE.

    Without this override, dbt would generate schema names like:
        dev_bronze, dev_silver, dev_gold (when target schema is 'dev')

    With this override, schemas are exactly:
        bronze, silver, gold, raw_data

    This is the standard production override recommended by dbt Labs.

    Reference: https://docs.getdbt.com/docs/build/custom-schemas
*/

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
