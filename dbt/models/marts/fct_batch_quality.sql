{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
        'field': 'arrived_at',
        'data_type': 'timestamp',
        'granularity': 'month'
    },
    cluster_by=['plant_id', 'fruit_type'],
    unique_key=['plant_id', 'batch_id', 'fruit_type']
) }}

select
    md5(concat(plant_id, '-', batch_id, '-', fruit_type)) as logistics_pk,
    *
from {{ ref('int_batch_quality') }}
{% if is_incremental() %}
where arrived_at > timestamp_trunc(timestamp '{{ var("start_date", "2021-01-01") }}', month)
{% endif %}
