{{
    config(
        materialized='table'
    )
}}

with wa as (

    select * from {{ ref('int_wa_members') }}

),

ghl as (

    select * from {{ ref('stg_ghl_contacts') }}

)

select
    wa.user_phone,
    wa.user_phone_hash,
    wa.group_name,
    wa.joined_at,
    wa.left_at,
    wa.duration_in_group,
    wa.last_event_type,
    wa.status,
    wa.duration_category,
    coalesce(g.contact_id,       ng.contact_id)       as ghl_contact_id,
    coalesce(g.first_name,       ng.first_name)       as ghl_first_name,
    coalesce(g.last_name,        ng.last_name)        as ghl_last_name,
    coalesce(g.email,            ng.email)            as ghl_email,
    coalesce(g.tags,             ng.tags)             as ghl_tags,
    coalesce(g.created_at,       ng.created_at)       as ghl_created_at,
    coalesce(g.last_activity_at, ng.last_activity_at) as ghl_last_activity_at,
    case
        when g.contact_id  is not null then 'phone'
        when ng.contact_id is not null then 'name'
    end as ghl_match_type
from wa
full outer join ghl g
    on wa.normalized_phone = g.normalized_phone
left join ghl ng
    on  g.contact_id is null
    and wa.user_phone is not null
    and wa.placeholder_name is not null
    and ng.is_unique_name
    and (   wa.placeholder_name = ng.normalized_name_fwd
         or wa.placeholder_name = ng.normalized_name_rev)
