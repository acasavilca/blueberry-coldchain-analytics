{{ config(
    materialized='table',
        partition_by={
        "field": "datetime",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

select
    cast(datetime as timestamp) as datetime,
    cast(T2M as float64) as temp_2m,
    cast(RH2M as float64) as rh_2m,
    cast(WS10M as float64) as wind_speed_10m,
    cast(ALLSKY_SFC_SW_DWN as float64) as shortwave_radiation,
    cast(T2MDEW as float64) as dew_point_2m,
    cast(PS as float64) as pressure,
    cast(TSOIL_54CM as float64) as t_soil_54cm
from {{ source('raw_data', 'satellite_ext') }}