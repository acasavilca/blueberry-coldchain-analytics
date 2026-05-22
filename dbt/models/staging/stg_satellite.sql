{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'satellite') }}
),

renamed as (
    select
        -- identifiers
        cast(location_id as string) as location_id,
        cast(timezone_id as string) as timezone_id,

        -- coordinates
        cast(latitude as float64) as latitude,
        cast(longitude as float64) as longitude,

        -- timestamps
        cast(datetime as timestamp) as measured_at,
        cast(loaded_at as timestamp) as loaded_at,

        -- telemetry
        cast(T2M as float64) as temp_2m_c,
        cast(RH2M as float64) as rh_2m_pct,
        cast(WS10M as float64) as wind_speed_10m,
        cast(ALLSKY_SFC_SW_DWN as float64) as shortwave_radiation_w_m2,
        cast(T2MDEW as float64) as dew_point_2m_c,
        cast(PS as float64) as pressure_kpa,
        cast(TSOIL_54CM as float64) as temp_soil_54cm_c
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
  and measured_at is not null
