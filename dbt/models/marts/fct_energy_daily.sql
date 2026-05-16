{{ config(
    materialized='incremental',
    partition_by={
        'field': 'measured_date_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['plant_id', 'fruit_type'],
    incremental_strategy='merge',
    unique_key=['plant_id', 'fruit_type', 'measured_date_at']
) }}

select
    * except(loaded_at)
from {{ ref('int_energy_daily') }}
where cop_daily >= 1
and compressor_duty_cycle > 0
{% if is_incremental() %}
and loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
