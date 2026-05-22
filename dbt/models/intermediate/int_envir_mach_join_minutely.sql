{{ config(
    materialized='view',
    partition_by={
        'field': 'measured_at',
        'data_type': 'timestamp',
        'granularity': 'month'
    }
) }}

{% set timestamp_columns = ["unflt.measured_at"] %}

select
    unflt.plant_id,
    unflt.fruit_type,
    unflt.measured_at,
    {{ get_localtime(timestamp_columns, 'timezone_id') }}
    unflt.loaded_at,
    unflt.temp_room_c,
    unflt.temp_pulp_c,
    unflt.rh_room_pct,
    unflt.co2_ppm,
    unflt.o2_pct,
    unflt.power_compressor_kw,
    unflt.compressor_on,
    unflt.humidifier_on,
    unflt.fruit_mass_stored_kg,
    unflt.temp_ambient_c,
    unflt.rh_ambient_pct,
    flt.q_total_w as q_total_w,
    flt.power_compressor_w as power_compressor_w_flt,
    (flt.temp_evap_inlet_c - flt.temp_evap_outlet_c) as coil_delta_t,
    (flt.rh_evap_inlet_pct - flt.rh_evap_outlet_pct) as coil_delta_rh,
    (flt.w_evap_inlet - flt.w_evap_outlet) * 1000 as coil_moisture_removed_g_kg,
    (flt.airflow_evap_kg_s * (flt.w_evap_inlet - flt.w_evap_outlet) * 60) as total_water_condensed_liters,
    flt.temp_coil_suction_c
from {{ ref('int_environmental_conditions_minutely') }} as unflt
left join {{ ref('int_machine_performance_minutely') }} as flt
    on unflt.plant_id = flt.plant_id
    and unflt.fruit_type = flt.fruit_type
    and unflt.measured_at = flt.measured_at
