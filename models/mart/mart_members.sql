{{
    config(
        materialized='table'
    )
}}

with events as (

    select * from {{ ref('stg_whatsapp_logs') }}

),

latest_event as (

    select
        user_phone_hash,
        group_name,
        event_type,
        timestamp,
        row_number() over (
            partition by user_phone_hash, group_name
            order by timestamp desc
        ) as rn
    from events

),

members as (

    select
        e.user_phone_hash,
        e.group_name,
        min(case when e.event_type in ('joined', 'added') then e.timestamp end) as joined_at,
        max(case when e.event_type = 'left' then e.timestamp end)               as left_at,
        le.event_type                                                            as last_event_type,
        case
            when le.event_type in ('joined', 'added') then 'member'
            else 'left'
        end                                                                      as status
    from events e
    inner join latest_event le
        on  e.user_phone_hash = le.user_phone_hash
        and e.group_name      = le.group_name
        and le.rn = 1
    group by
        e.user_phone_hash,
        e.group_name,
        le.event_type

),
added_statistics as (
    select
        user_phone_hash,
        group_name,
        joined_at,
        left_at,
        coalesce(
            left_at,
            current_timestamp
        ) - joined_at as duration_in_group,
        last_event_type,
        status
    from members
)

select
    *,
    case
        when left_at is null then '1. Miembro activo'
        when duration_in_group < interval '1 day' then '5. Duro menos de un día'
        when duration_in_group < interval '1 week' then '4. Duro menos de una semana'
        when duration_in_group < interval '1 month' then '3. Duro menos de un mes'
        else '2. Duro más de un mes'
    end as duration_category
from added_statistics