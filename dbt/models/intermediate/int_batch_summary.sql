{{ config(
    materialized='table',
) }}

select
    plant_id,
    fruit_type,
    quality_grade,
    date(arrived_at) as arrival_date,
    timestamp_diff(dispatched_final_at, arrived_at, minute) / 60.0 as residence_hours,
    mass_kg_initial,
    mass_kg_initial - mass_kg_remaining as weight_loss_kg,
    safe_divide(mass_kg_initial - mass_kg_remaining, mass_kg_initial) * 100 as weight_loss_pct,
    tunnel_exit_temp_c
from {{ ref('stg_batches') }}
where dispatched_final_at is not null
