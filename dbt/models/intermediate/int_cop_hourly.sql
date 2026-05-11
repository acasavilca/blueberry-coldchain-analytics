{{ config(
    materialized='view',
) }}

with data_for_cop as (
    select
        *
    from {{ ref('int_data_for_cop_hourly') }}
)

select
    plant_id,
    fruit_type,
    measured_hour_at,
    measured_at_localtime,
    loaded_at,
    temp_2m_c as t_ambient_c,
    one_hour_duty_cycle_compressor,
    {{ calculate_cop(
        't_evap_inlet_c',
        't_evap_outlet_c',
        'rh_evap_inlet_pct',
        'rh_evap_outlet_pct',
        'pressure_pa',
        'airflow_evap_kg_s',
        'power_compressor_w'
    ) }} as cop_hourly
from data_for_cop