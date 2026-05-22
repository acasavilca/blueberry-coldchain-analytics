{{ config(
    materialized='table'
) }}

select * from {{ ref('fruit_specific_params') }}