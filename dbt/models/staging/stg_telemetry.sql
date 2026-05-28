{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'telemetry') }}
),

renamed as (
    select
        -- identifiers
        cast(plant_id as string) as plant_id,
        cast(fruit_type as string) as fruit_type,
        cast(timezone_id as string) as timezone_id,

        -- timestamps
        cast(datetime as timestamp) as measured_at,
        cast(loaded_at as timestamp) as loaded_at,

        -- telemetry data
        cast(temp_room_c as float64) as temp_room_c,
        cast(temp_pulp_c as float64) as temp_pulp_c,
        cast(rh_room_pct as float64) as rh_room_pct,
        cast(co2_ppm as float64) as co2_ppm,
        cast(o2_pct as float64) as o2_pct,
        cast(power_compressor_kw as float64) as power_compressor_kw,
        cast(temp_evap_inlet_c as float64) as temp_evap_inlet_c,
        cast(temp_evap_outlet_c as float64) as temp_evap_outlet_c,
        cast(rh_evap_inlet_pct as float64) as rh_evap_inlet_pct,
        cast(rh_evap_outlet_pct as float64) as rh_evap_outlet_pct,
        cast(evap_fan_speed_pct as float64) as evap_fan_speed_pct,
        cast(temp_coil_suction_c as float64) as temp_coil_suction_c,
        cast(fruit_mass_stored_kg as float64) as fruit_mass_stored_kg,
        cast(comp_modulation_pct as float64) as comp_modulation_pct,

        cast(door_int_open as int64) as door_int_open,
        cast(door_ext_open as int64) as door_ext_open,
        cast(compressor_on as int64) as compressor_on,
        cast(humidifier_on as int64) as humidifier_on
    from source
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by plant_id, fruit_type, measured_at
            order by loaded_at desc
        ) as row_num
    from renamed
)

select
    * except(row_num)  
from deduplicated
where row_num = 1
  and plant_id is not null
  and fruit_type is not null
  and measured_at is not null
