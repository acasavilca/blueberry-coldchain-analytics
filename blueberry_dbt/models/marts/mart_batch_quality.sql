{{ config(
    materialized='table',
) }}

select
    plant_id,
    fruit_type,
    quality_grade,
    date_trunc(arrival_date, month) as month,
    count(*) as batch_count,
    sum(mass_kg_initial) as total_mass_kg,
    avg(weight_loss_pct) as avg_weight_loss_pct,
    avg(residence_hours) as avg_residence_hours,
    avg(tunnel_exit_temp_c) as avg_tunnel_exit_temp_c
from {{ ref('int_batch_summary') }}
group by 1, 2, 3, 4