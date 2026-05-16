{{ config(
    materialized='incremental',
    partition_by={
        'field': 'arrived_at',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by=['plant_id', 'fruit_type'],
    incremental_strategy='merge',
    unique_key=['plant_id', 'batch_id', 'fruit_type']
) }}

select
    * except(loaded_at)
from {{ ref('int_batch_quality') }}
{% if is_incremental() %}
where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
