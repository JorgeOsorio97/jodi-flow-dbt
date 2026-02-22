
with stg_ads as

(
    select * from {{ ref('stg_ads_insights') }}
),
unique_ads as (
    select distinct ad_id, ad_name, campaign_name
    from stg_ads
),
group_numbers_estimations as (

    select
        ad_id,
        ad_name,
        cast(substring(ad_name from '\d+') as integer) as group_number_ad,
        campaign_name,
        cast(substring(campaign_name from '\d+') as integer) as group_number_campaign
    from unique_ads

),
ads_with_group_number as (
    select
        ads.*,
        coalesce(
            group_numbers.group_number_ad, 
            group_numbers.group_number_campaign
        ) as group_number
    from stg_ads ads
        left join group_numbers_estimations group_numbers
        on ads.ad_id = group_numbers.ad_id
)
select * from ads_with_group_number
