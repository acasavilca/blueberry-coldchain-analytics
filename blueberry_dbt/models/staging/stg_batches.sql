{{ config(
    materialized='table',
        partition_by={
        'field': 'arrival_ts',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['fruit_type', 'quality_grade']
) }}

select
    -- identifiers
    cast(plant_id as string) as plant_id,
    cast(batch_id as int64) as batch_id,
    cast(fruit_type as string) as fruit_type,
    cast(quality_grade as string) as quality_grade,

    -- timestamps
    cast(arrival_ts as timestamp) as arrival_ts,
    cast(first_dispatch_ts as timestamp) as first_dispatch_ts,
    cast(final_dispatch_ts as timestamp) as final_dispatch_ts,

    cast(mass_kg_initial as float64) as mass_kg_initial,
    cast(mass_kg_remaining as float64) as mass_kg_remaining,
    cast(tunnel_exit_temp_c as float64) as tunnel_exit_temp_c

from {{ source('raw_data', 'batches_ext') }}