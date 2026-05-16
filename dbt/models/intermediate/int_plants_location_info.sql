{{ config(
    materialized='view',
) }}

with weather as (
    select distinct
        location_id,
        latitude,
        longitude
    from {{ ref('stg_satellite') }}
),

telemetry as (
    select distinct
        plant_id,
        timezone_id
    from {{ ref('stg_telemetry') }}
)

select
    t.plant_id,
    t.timezone_id,
    w.latitude,
    w.longitude
from weather w
join telemetry t
    on w.location_id = t.plant_id