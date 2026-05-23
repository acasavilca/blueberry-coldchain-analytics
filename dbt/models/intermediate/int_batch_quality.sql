{{ config(
    materialized='view',
) }}

{% set timestamp_columns = ["b.arrived_at", "b.dispatched_final_at"] %}

with batch_telemetry_agg as (
    select
        plant_id,
        batch_id,
        fruit_type,
        quality_grade,
        arrived_at,
        dispatched_final_at,
        residence_time,
        tunnel_exit_temp_c,
        countif(temp_damage_check = 'potential_heat_damage') as minutes_heat_damage,
        countif(temp_damage_check = 'potential_freeze_damage') as minutes_chill_damage,
        countif(temp_damage_check = 'safe_temperature') as minutes_safe
    from {{ ref('int_batch_telemetry_interpolated') }}
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

fruits_thresholds as (
    select
        fruit_type,
        heat_damage_minutes_threshold,
        chill_damage_minutes_threshold
    from {{ ref('fruit_specific_params') }}
),

plant_info as (
    select
        plant_id,
        timezone_id
    from {{ ref('int_plants_location_info') }}
)

select
    b.plant_id,
    b.batch_id,
    b.fruit_type,
    b.quality_grade,
    b.arrived_at,
    b.dispatched_final_at,
    {{ get_localtime(timestamp_columns, 'pl.timezone_id') }}
    b.residence_time,
    b.tunnel_exit_temp_c,
    {{ has_temp_damage_flag('b.minutes_heat_damage', 'ft.heat_damage_minutes_threshold') }} as has_heat_damage,
    {{ has_temp_damage_flag('b.minutes_chill_damage', 'ft.chill_damage_minutes_threshold') }} as has_chill_damage,
    b.minutes_heat_damage,
    b.minutes_chill_damage
from batch_telemetry_agg as b
join fruits_thresholds as ft on b.fruit_type = ft.fruit_type
join plant_info as pl on b.plant_id = pl.plant_id
