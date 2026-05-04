{{ config(materialized='view') }}

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

        -- timestamps
        cast(arrival_ts as datetime) as arrived_at,
        cast(first_dispatch_ts as datetime) as dispatched_first_at,
        cast(final_dispatch_ts as datetime) as dispatched_final_at,
        cast(loaded_at as timestamp) as loaded_at,

        cast(mass_kg_initial as float64) as mass_kg_initial,
        cast(mass_kg_remaining as float64) as mass_kg_remaining,
        cast(tunnel_exit_temp_c as float64) as tunnel_exit_temp_c
    from source
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by plant_id, batch_id
            order by loaded_at desc
        ) as row_num
    from renamed
)

select
    * except(row_num)  
from deduplicated
where row_num = 1
