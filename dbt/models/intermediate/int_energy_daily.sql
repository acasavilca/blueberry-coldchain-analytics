{{ config(
    materialized='view',
) }}

with telemetry_daily as (
    select
        plant_id,
        fruit_type,
        timestamp_trunc(measured_hour_at, day) as measured_date_at,
        -- datetime_trunc(measured_hour_at_localtime, day) as measured_date_at_localtime,

        max(loaded_at) as loaded_at,
        avg(t_ambient_c) as t_ambient_c,
        avg(compressor_duty_cycle) as compressor_duty_cycle,
        avg(compressor_power_kw) as compressor_power_kw,
        sum(compressor_energy_kwh) as compressor_energy_kwh,
        safe_divide(sum(cop_hourly * compressor_energy_kwh), sum(compressor_energy_kwh)) as cop_daily,
        avg(fruit_mass_stored_kg) as fruit_mass_stored_kg
    from {{ ref('int_cop_hourly') }}
    group by 1, 2, 3
)

select
    *,
    safe_divide(compressor_energy_kwh, fruit_mass_stored_kg) as energy_per_kg_fruit
from telemetry_daily
