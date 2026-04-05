/*
    Snapshot: taxi_zones_snapshot
    Type: SCD Type 2 (Slowly Changing Dimension)
    Description: Tracks historical changes to NYC Taxi zone reference data.
                 Captures when zone names, boroughs, or service zones are updated
                 by the TLC. Required for point-in-time accurate reporting —
                 e.g., if a zone moved from one borough to another, historical
                 trip revenue should reflect the zone assignment at time of trip.

    Usage:
        dbt snapshot --select taxi_zones_snapshot

    Result columns added by dbt:
        dbt_scd_id        - unique row identifier (hash of PK + dbt_updated_at)
        dbt_updated_at    - when this snapshot row was last evaluated
        dbt_valid_from    - when this version of the record became active
        dbt_valid_to      - when this version was superseded (NULL = current)
*/

{% snapshot taxi_zones_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='zone_id',
        strategy='check',
        check_cols=['zone_name', 'borough', 'service_zone', 'is_airport', 'borough_code'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    zone_id,
    zone_name,
    borough,
    borough_code,
    service_zone,
    is_airport,
    CURRENT_TIMESTAMP() AS snapshot_extracted_at

FROM {{ ref('silver_taxi_zones') }}

{% endsnapshot %}
