{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'events') }}
),

renamed as (
    select
        cast(plant_id as string) as plant_id,
        cast(timestamp as timestamp) as event_at,
        cast(loaded_at as timestamp) as loaded_at,
        cast(event_type as string) as event_type,
        cast(batch_id as int64) as batch_id,
        cast(mass_kg as float64) as mass_kg,
        cast(fruit_type as string) as fruit_type,
        cast(quality_grade as string) as quality_grade,
        cast(tunnel_exit_temp_c as float64) as tunnel_exit_temp_c
    from source
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by plant_id, fruit_type, batch_id, event_at, event_type
            order by loaded_at desc
        ) as row_num
    from renamed
)

select
    * except(row_num)  
from deduplicated
where row_num = 1