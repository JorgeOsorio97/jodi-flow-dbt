{{
    config(
        materialized='table'
    )
}}
with src as (
select 
    timestamp,
    user_phone_hash,
    group_name,
    cast(substring(group_name from '\d+') as integer) as group_number,
    event_type,
    case when event_type in ('joined', 'added') then 1
         else -1
    end as event_value,
    sum(case when event_type in ('joined', 'added') then 1
         else -1
    end) over (
        partition by group_name
        order by timestamp
        rows between unbounded preceding and current row
    ) as rolling_member_count,

    timestamp = min(timestamp) over (
        partition by group_name, cast(timestamp as date)
        order by timestamp
        rows between unbounded preceding and unbounded following
    ) as is_first_event_of_day,

    timestamp = max(timestamp) over (
        partition by group_name, cast(timestamp as date)
        order by timestamp
        rows between unbounded preceding and unbounded following
    ) as is_last_event_of_day

from {{ source('whatsapp','raw_whatsapp_logs') }}
)
select * from src