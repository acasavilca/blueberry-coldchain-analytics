{{ config(
    materialized='table',
    cluster_by=['plant_id']
) }}

select * from {{ ref('int_plants_location_info') }}
