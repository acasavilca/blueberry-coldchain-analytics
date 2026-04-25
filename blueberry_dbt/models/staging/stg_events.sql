{{ config(
    materialized='table',
        partition_by={
        "field": "timestamp",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=['fruit_type', 'quality_grade']
) }}

select
    cast(plant_id as string) as plant_id,
    cast(timestamp as timestamp) as timestamp,
    cast(event_type as string) as event_type,
    cast(batch_id as int64) as batch_id,
    cast(mass_kg as float64) as mass_kg,
    cast(fruit_type as string) as fruit_type,
    cast(quality_grade as string) as quality_grade,
    cast(tunnel_exit_temp_c as float64) as tunnel_exit_temp_c
from {{ source('raw_data', 'events_ext') }}