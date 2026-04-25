{{ config(
    materialized='table',
) }}

select
    plant_id,
    fruit_type,
    quality_grade,
    date(arrival_ts) as arrival_date,
    timestamp_diff(final_dispatch_ts, arrival_ts, minute) / 60.0 as residence_hours,
    mass_kg_initial,
    mass_kg_initial - mass_kg_remaining as weight_loss_kg,
    safe_divide(mass_kg_initial - mass_kg_remaining, mass_kg_initial) * 100 as weight_loss_pct,
    tunnel_exit_temp_c
from {{ ref('stg_batches') }}
where final_dispatch_ts is not null