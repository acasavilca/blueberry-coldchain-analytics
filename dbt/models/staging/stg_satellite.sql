{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'satellite') }}
),

renamed as (
    select
        cast(datetime as datetime) as measured_at,
        cast(loaded_at as timestamp) as loaded_at,
        cast(T2M as float64) as temp_2m,
        cast(RH2M as float64) as rh_2m,
        cast(WS10M as float64) as wind_speed_10m,
        cast(ALLSKY_SFC_SW_DWN as float64) as shortwave_radiation,
        cast(T2MDEW as float64) as dew_point_2m,
        cast(PS as float64) as pressure,
        cast(TSOIL_54CM as float64) as t_soil_54cm
    from source
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by measured_at
            order by loaded_at desc
        ) as row_num
    from renamed
)

select
    * except(row_num)  
from deduplicated
where row_num = 1
