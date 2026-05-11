{{ config(
    materialized='incremental',
    partition_by={
        'field': 'measured_hour_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['plant_id', 'fruit_type'],
    incremental_strategy='merge',
    unique_key=['plant_id', 'fruit_type', 'measured_hour_at']
) }}

with hourly_cop_data as (
    select
        plant_id,
        fruit_type,
        measured_hour_at as hour_at,
        measured_at_localtime as hour_at_local,
        t_ambient_c as ambient_temp_c,
        one_hour_duty_cycle_compressor as compressor_duty_cycle,
        cop_hourly as cop
    from {{ ref('int_cop_hourly') }}
)

select * from hourly_cop_data
order by fruit_type desc, hour_at

{% if is_incremental() %}
    where hour_at > (select max(hour_at) from {{ this }})
{% endif %}
