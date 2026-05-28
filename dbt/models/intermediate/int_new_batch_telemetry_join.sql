{{ config(
    materialized='view',
) }}

with telemetry as (
    select
        plant_id,
        fruit_type,
        measured_at,
        temp_pulp_c,
        temp_room_c,
        rh_room_pct,
        door_int_open,
        door_ext_open
    from {{ ref('stg_telemetry') }}
),

fruit_specific_params as (
    select
        fruit_type,
        chill_damage_temp_c,
        heat_damage_temp_c,
        rh_low_threshold,
        rh_high_threshold,
        heat_damage_minutes_threshold,
        chill_damage_minutes_threshold,
        rh_low_damage_minutes_threshold
    from {{ ref('fruit_specific_params') }}
),

batches_events as (
    select
        plant_id,
        batch_id,
        fruit_type,
        quality_grade,
        arrived_at,
        dispatched_final_at,
        residence_time,
        tunnel_exit_temp_c
    from {{ ref('int_batch_events') }}
),

batch_telemetry_join as (
    select
        be.plant_id,
        be.batch_id,
        be.fruit_type,
        md5(concat(be.plant_id, '-', be.batch_id, '-', be.fruit_type)) as batch_pk,
        be.quality_grade,
        tel.measured_at,
        {{ parameter_abuse_flag('tel.temp_pulp_c', 'ft.chill_damage_temp_c', 'ft.heat_damage_temp_c') }} as temp_pulp_status,
        {{ parameter_abuse_flag('tel.rh_room_pct', 'ft.rh_low_threshold', 'ft.rh_high_threshold') }} as rh_status,
        tel.temp_room_c,
        tel.temp_pulp_c,
        tel.rh_room_pct,
        tel.door_int_open,
        tel.door_ext_open,
        be.arrived_at,
        be.dispatched_final_at,
        be.residence_time,
        be.tunnel_exit_temp_c
    from batches_events as be
    join telemetry as tel
        on be.plant_id = tel.plant_id
        and be.fruit_type = tel.fruit_type
        and tel.measured_at between be.arrived_at and be.dispatched_final_at
    join fruit_specific_params as ft
        on be.fruit_type = ft.fruit_type
),

temp_pulp_status_filtered as (
    select
        batch_pk,
        measured_at,
        temp_pulp_status,
        {{ route_valid_duration('temp_pulp_status', "'low'", 'measured_at', 15, 'minute', 'batch_pk') }} as minutes_at_low_temp_pulp,
        {{ route_valid_duration('temp_pulp_status', "'high'", 'measured_at', 15, 'minute', 'batch_pk') }} as minutes_at_high_temp_pulp,
        {{ route_downtime_duration('temp_pulp_status', 'measured_at', 15, 'minute', 'batch_pk') }} as minutes_unknown_temp_downtime,
    from batch_telemetry_join
    where temp_pulp_status is not null
),

rh_status_filtered as (
    select
        batch_pk,
        measured_at,
        rh_status,
        {{ route_valid_duration('rh_status', "'low'", 'measured_at', 15, 'minute', 'batch_pk') }} as minutes_at_low_rh,
        {{ route_valid_duration('rh_status', "'high'", 'measured_at', 15, 'minute', 'batch_pk') }} as minutes_at_high_rh,
        {{ route_downtime_duration('rh_status', 'measured_at', 15, 'minute', 'batch_pk') }} as minutes_unknown_rh_downtime
    from batch_telemetry_join
    where rh_status is not null
),

door_int_status_filtered as (
    select
        batch_pk,
        measured_at,
        {{ route_valid_duration('door_int_open', 1, 'measured_at', 10, 'minute', 'batch_pk') }} as minutes_door_int_open
    from batch_telemetry_join
    where door_int_open is not null
),

door_ext_status_filtered as (
    select
        batch_pk,
        measured_at,
        {{ route_valid_duration('door_ext_open', 1, 'measured_at', 10, 'minute', 'batch_pk') }} as minutes_door_ext_open
    from batch_telemetry_join
    where door_ext_open is not null
)

select
    b.batch_pk,
    b.plant_id,
    b.batch_id,
    b.fruit_type,
    b.quality_grade,
    b.arrived_at,
    b.dispatched_final_at,
    b.residence_time,
    b.tunnel_exit_temp_c,
    sum(tp_filt.minutes_at_low_temp_pulp) as minutes_at_low_temp_pulp,
    sum(tp_filt.minutes_at_high_temp_pulp) as minutes_at_high_temp_pulp,
    sum(tp_filt.minutes_unknown_temp_downtime) as minutes_unknown_temp_downtime,
    sum(rh_filt.minutes_at_low_rh) as minutes_at_low_rh,
    sum(rh_filt.minutes_at_high_rh) as minutes_at_high_rh,
    sum(rh_filt.minutes_unknown_rh_downtime) as minutes_unknown_rh_downtime,
    sum(d_int_filt.minutes_door_int_open) as minutes_door_int_open,
    sum(d_ext_filt.minutes_door_ext_open) as minutes_door_ext_open,
    max(b.temp_room_c) as max_temp_room_c,
    min(b.temp_room_c) as min_temp_room_c,
    avg(b.temp_room_c) as avg_temp_room_c,
    max(b.temp_pulp_c) as max_temp_pulp_c,
    min(b.temp_pulp_c) as min_temp_pulp_c,
    avg(b.temp_pulp_c) as avg_temp_pulp_c,
    max(b.rh_room_pct) as max_rh_room_pct,
    min(b.rh_room_pct) as min_rh_room_pct,
    avg(b.rh_room_pct) as avg_rh_room_pct
from batch_telemetry_join as b
left join temp_pulp_status_filtered as tp_filt
    on b.batch_pk = tp_filt.batch_pk
    and b.measured_at = tp_filt.measured_at
left join rh_status_filtered as rh_filt
    on b.batch_pk = rh_filt.batch_pk
    and b.measured_at = rh_filt.measured_at
left join door_int_status_filtered as d_int_filt
    on b.batch_pk = d_int_filt.batch_pk
    and b.measured_at = d_int_filt.measured_at
left join door_ext_status_filtered as d_ext_filt
    on b.batch_pk = d_ext_filt.batch_pk
    and b.measured_at = d_ext_filt.measured_at
group by 1, 2, 3, 4, 5, 6, 7, 8, 9

-- where cold_detection = 'to_low' or cold_detection = 'from_low'
