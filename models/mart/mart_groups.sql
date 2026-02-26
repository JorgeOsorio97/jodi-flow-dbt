{{
    config(
        materialized='table'
    )
}}

with members as (

    select * from {{ ref('mart_members') }}

)

select
    cast(substring(group_name from '\d+') as integer) as group_id,
    group_name,
    count(case when status = 'member' then 1 end)       as current_member_count,
    count(case when joined_at is not null then 1 end)    as historic_member_count,
    min(joined_at)                                       as first_event_timestamp,
    greatest(max(joined_at), max(left_at))               as last_event_timestamp,
    max(joined_at)                                       as last_joined_timestamp,
    max(left_at)                                         as last_left_timestamp

from members
group by group_name
