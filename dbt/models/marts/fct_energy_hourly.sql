{# {% set partitions_to_replace = [
    'measured_hour',
    ''
]%} #}

{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        'field': 'hour_utc',
        'data_type': 'timestamp',
        'granularity': 'month'
    },
    cluster_by=['plant_id', 'fruit_type']
) }}

with minutely_data as (
    select
        plant_id,
        fruit_type,
        timestamp_trunc(measured_at, hour) as hour_utc,
        timestamp_trunc(measured_at_localtime, hour) as hour_localtime,
        temp_room_c,
        temp_pulp_c,
        rh_room_pct,
        co2_ppm,
        o2_pct,
        power_compressor_kw,
        compressor_on,
        humidifier_on,
        q_total_w,
        power_compressor_w_flt,
        temp_coil_suction_c,
        coil_delta_t,
        coil_delta_rh,
        coil_moisture_removed_g_kg,
        total_water_condensed_liters,
        fruit_mass_stored_kg
    from {{ ref('int_envir_mach_join_minutely') }}
    {% if is_incremental() %}
    where measured_at >= timestamp '{{ var("start_date", "2021-01-01") }}'
    {% endif %}
),

hourly_aggregation as (
    select
        md5(concat(plant_id, '-', fruit_type, '-', hour_utc)) as energy_hourly_pk,
        plant_id,
        fruit_type,
        hour_utc,
        min(hour_localtime) as hour_localtime,
        avg(temp_room_c) as temp_room_c,
        avg(temp_pulp_c) as temp_pulp_c,
        avg(rh_room_pct) as rh_room_pct,
        avg(co2_ppm) as co2_ppm,
        avg(o2_pct) as o2_pct,
        sum(power_compressor_kw/60) as power_compressor_kwh,
        safe_divide(
            countif(compressor_on = 1),
            count(compressor_on)
        ) as compressor_duty_cycle_hourly,
        safe_divide(
            countif(humidifier_on = 1),
            count(humidifier_on)
        ) as humidifier_duty_cycle_hourly,
        safe_divide(
            sum(q_total_w),
            sum(power_compressor_w_flt)
        ) as cop_hourly,
        avg(temp_coil_suction_c) as temp_coil_suction_c,
        avg(coil_delta_t) as coil_delta_t,
        avg(coil_delta_rh) as coil_delta_rh,
        avg(coil_moisture_removed_g_kg) as coil_moisture_removed_g_kg,
        sum(total_water_condensed_liters) as total_water_condensed_liters,
        avg(fruit_mass_stored_kg) as avg_fruit_mass_stored_kg,
        max(fruit_mass_stored_kg) as max_fruit_mass_stored_kg,
        min(fruit_mass_stored_kg) as min_fruit_mass_stored_kg
    from minutely_data
    group by 1, 2, 3, 4
)

select * from hourly_aggregation
-- where fruit_type = 'avocado'
-- order by cop_hourly desc
