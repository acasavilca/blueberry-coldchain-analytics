{{ config(
    materialized='table',
) }}

select
    e.plant_id,
    e.fruit_type,
    e.date,
    e.daily_compressor_energy_kwh,
    e.daily_one_hour_duty_cycle_cooling,
    case 
        when avg(ev.cop) > 2.5 then null
        when avg(ev.cop) < 0 then 0
        else avg(ev.cop) end as daily_avg_sensible_cop,
    avg(ev.q_sensible_kw) as daily_avg_q_sensible_kw
from {{ ref('int_energy_daily') }} e
left join {{ ref('int_evaporator_hourly') }} ev
    on e.plant_id = ev.plant_id
    and e.fruit_type = ev.fruit_type
    and e.date = date(ev.date)
where ev.w_compressor_kw > 0.01
and ev.cop <= 2.5 
group by 1, 2, 3, 4, 5