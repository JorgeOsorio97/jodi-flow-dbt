{{
    config(
        materialized='view'
    )
}}
with src as (
select * from {{ source('meta_ads','ads_insights') }}
where date_start >= '2025-01-01'
    and account_name='JoDi Promos Mexico' 
)
select * from src
