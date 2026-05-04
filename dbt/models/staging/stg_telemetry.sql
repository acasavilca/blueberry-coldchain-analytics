{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'telemetry') }}
),

renamed as (
    select
        -- identifiers
        cast(plant_id as string) as plant_id,
        cast(fruit_type as string) as fruit_type,

        -- timestamps
        cast(datetime as datetime) as measured_at,
        cast(loaded_at as timestamp) as loaded_at,

        -- telemetry data
        cast(T_room as float64) as t_room,
        cast(T_pulp as float64) as t_pulp,
        cast(RH_room as float64) as rh_room,
        cast(CO2_ppm as float64) as co2_ppm,
        cast(O2_pct as float64) as o2_pct,
        cast(W_compressor_kw as float64) as w_compressor_kw,
        cast(T_evap_in_air as float64) as t_evap_in_air,
        cast(T_evap_out_air as float64) as t_evap_out_air,
        cast(RH_evap_out as float64) as rh_evap_out,
        cast(m_dot_evap_air_kg_s as float64) as m_dot_evap_air_kg_s,
        cast(cooling_call as int64) as cooling_call,
        cast(humidifier_call as int64) as humidifier_call
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
