{{ config(materialized='view') }}

{% set timestamp_columns = ["arrived_at", "dispatched_first_at", "dispatched_final_at"] %}

with source as (
    select * from {{ source('raw_data', 'batches') }}
),

renamed as (
    select
        -- identifiers
        cast(plant_id as string) as plant_id,
        cast(batch_id as int64) as batch_id,
        cast(fruit_type as string) as fruit_type,
        cast(quality_grade as string) as quality_grade,
        cast(timezone_id as string) as timezone_id,

        -- timestamps
        cast(arrival_ts as timestamp) as arrived_at,
        cast(first_dispatch_ts as timestamp) as dispatched_first_at,
        cast(final_dispatch_ts as timestamp) as dispatched_final_at,
        cast(loaded_at as timestamp) as loaded_at,

        cast(mass_kg_initial as float64) as mass_kg_initial,
        cast(mass_kg_remaining as float64) as mass_kg_remaining,
        cast(tunnel_exit_temp_c as float64) as tunnel_exit_temp_c
    from source
),

deduplicated as (
    select
        *,
        {{ get_localtime(timestamp_columns, 'timezone_id') }}
        row_number() over (
            partition by plant_id, batch_id, fruit_type
            order by loaded_at desc
        ) as row_num
    from renamed
)

select
    * except(row_num)
from deduplicated
where row_num = 1
