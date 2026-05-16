{{ config(
    materialized='view',
) }}

{% set timestamp_columns = ["tel.measured_hour_at"] %}

with telemetry_cop_inputs as (
    select
        *
    from {{ ref('int_telemetry_cop_inputs') }}
),

fruits_specs as (
    select
        fruit_type,
        q_rated_kw*1000 as q_rated_w
    from {{ ref('fruit_thresholds') }}
),

raw_cop_hourly as (
    select
        plant_id,
        tel.fruit_type,
        measured_hour_at,
        loaded_at,
        temp_2m_c as t_ambient_c,
        compressor_duty_cycle,
        {{ old_calculate_cop(
            't_evap_inlet_c',
            't_evap_outlet_c',
            'rh_evap_inlet_pct',
            'rh_evap_outlet_pct',
            'pressure_pa',
            'airflow_evap_kg_s',
            'power_compressor_w',
            'compressor_duty_cycle',
            'ft.q_rated_w'
        ) }} as cop_hourly,
        airflow_evap_kg_s, -- REMOVE
        power_compressor_w,
        compressor_energy_kwh,
        fruit_mass_stored_kg
    from telemetry_cop_inputs tel
    join fruits_specs as ft on tel.fruit_type = ft.fruit_type
),

plant_info as (
    select
        plant_id,
        timezone_id
    from {{ ref('int_plants_location_info') }}
)

select
    tel.plant_id,
    tel.fruit_type,
    tel.measured_hour_at,
    {{ get_localtime(timestamp_columns, 'pl.timezone_id') }}
    tel.loaded_at,
    tel.t_ambient_c,
    tel.compressor_duty_cycle,
    tel.cop_hourly,
    tel.airflow_evap_kg_s, -- REMOVE
    tel.power_compressor_w/1000 as compressor_power_kw,
    tel.compressor_energy_kwh,
    tel.fruit_mass_stored_kg
from raw_cop_hourly as tel
join plant_info as pl
    on tel.plant_id = pl.plant_id
order by cop_hourly desc -- REMOVE
