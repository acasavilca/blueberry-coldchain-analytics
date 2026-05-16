{{ config(
    materialized='table'
) }}

select * from {{ ref('fruit_thresholds') }}