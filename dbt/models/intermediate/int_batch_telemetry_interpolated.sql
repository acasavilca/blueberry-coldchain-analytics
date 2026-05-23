{{ config(
    materialized='view',
) }}

with batch_interpolated as (
    select
        plant_id,
        batch_id,
        fruit_type,
        quality_grade,
        arrived_at,
        dispatched_final_at,
        residence_time,
        tunnel_exit_temp_c,
        generated_time,
        loaded_at
    from {{ ref('int_batch_events') }}
    cross join unnest(generate_timestamp_array(arrived_at, dispatched_final_at, interval 1 minute)) as generated_time
),

fruits_thresholds as (
    select
        fruit_type,
        heat_damage_temp_c,
        freeze_damage_temp_c
    from {{ ref('fruit_specific_params') }}
),

telemetry as (
    select
        tel.plant_id,
        tel.fruit_type,
        tel.temp_room_c,
        tel.temp_pulp_c,
        tel.measured_at,
        {{ temp_abuse_flag('tel.temp_pulp_c', 'ft.heat_damage_temp_c', 'ft.freeze_damage_temp_c') }} as temp_damage_check
    from {{ ref('stg_telemetry') }} tel
    join fruits_thresholds ft
        on tel.fruit_type = ft.fruit_type
)

select
    bi.plant_id,
    bi.batch_id,
    bi.fruit_type,
    bi.quality_grade,
    tel.temp_room_c,
    tel.temp_pulp_c,
    bi.arrived_at,
    bi.dispatched_final_at,
    bi.residence_time,
    bi.tunnel_exit_temp_c,
    bi.generated_time,
    tel.temp_damage_check,
    bi.loaded_at
from batch_interpolated bi
join telemetry tel
    on bi.plant_id = tel.plant_id
    and bi.fruit_type = tel.fruit_type
    and bi.generated_time = tel.measured_at
-- where bi.fruit_type = 'avocado'
