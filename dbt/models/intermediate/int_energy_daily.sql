{{ config(
    materialized='table',
) }}

select
    plant_id,
    fruit_type,
    cast(date as date) as date,
    sum(compressor_energy_kwh)  as daily_compressor_energy_kwh,
    avg(one_hour_duty_cycle_cooling) as daily_one_hour_duty_cycle_cooling,
    # avg(five_hour_duty_cycle_cooling) as daily_five_hour_duty_cycle_cooling,
    avg(one_hour_duty_cycle_humidifier) as daily_one_hour_duty_cycle_humidifier,
    # avg(five_hour_duty_cycle_humidifier) as daily_five_hour_duty_cycle_humidifier
from {{ ref('int_telemetry_hourly') }}
group by 1, 2, 3