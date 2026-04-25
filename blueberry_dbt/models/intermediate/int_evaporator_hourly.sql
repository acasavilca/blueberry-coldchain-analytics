{{ config(
    materialized='table',
) }}

select
    plant_id,
    fruit_type,
    timestamp_trunc(datetime, hour) as date,
    
    avg(1006.0 * m_dot_evap_air_kg_s * (t_evap_in_air - t_evap_out_air) / 1000) as q_sensible_kw,
    avg(w_compressor_kw) as w_compressor_kw,
    safe_divide(
        avg(1006.0 * m_dot_evap_air_kg_s * (t_evap_in_air - t_evap_out_air) / 1000),
        nullif(avg(w_compressor_kw), 0)
    ) as cop
from {{ ref('stg_telemetry') }}
where cooling_call = 1
-- and w_compressor_kw > 1e-9 -- filter out near-zero compressor power
group by 1, 2, 3