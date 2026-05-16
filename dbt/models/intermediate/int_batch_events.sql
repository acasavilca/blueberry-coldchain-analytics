{{ config(
    materialized='view',
) }}

with batches as (
    select
        plant_id,
        batch_id,
        fruit_type,
        quality_grade,
        arrived_at,
        dispatched_first_at,
        dispatched_final_at,
        loaded_at,
        (dispatched_final_at - arrived_at) as residence_time,
        mass_kg_initial,
        tunnel_exit_temp_c
    from {{ ref('stg_batches') }}
    where dispatched_final_at is not null
),

events as (
    select
        plant_id,
        batch_id,
        fruit_type,
        sum(mass_kg) as mass_kg_dispatched,
        count(*) as num_dispatch_events
    from {{ ref('stg_events') }}
    where event_type like 'dispatch%'
    group by 1, 2, 3
)

select 
    b.plant_id,
    b.batch_id,
    b.fruit_type,
    b.quality_grade,
    b.arrived_at,
    b.dispatched_first_at,
    b.dispatched_final_at,
    b.loaded_at,
    b.residence_time,
    b.tunnel_exit_temp_c,
    b.mass_kg_initial,
    e.mass_kg_dispatched,
    e.num_dispatch_events
from events e
inner join batches b
    on e.plant_id = b.plant_id
    and e.fruit_type = b.fruit_type
    and e.batch_id = b.batch_id
