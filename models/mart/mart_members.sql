{{
    config(
        materialized='table'
    )
}}

with wa as (

    select * from {{ ref('int_wa_members') }}

),

matches as (

    select * from {{ ref('int_wa_ghl_matches') }}

),

ghl as (

    select * from {{ ref('stg_ghl_contacts') }}

),

-- WA members with optional GHL enrichment (simple indexed left joins)
wa_enriched as (

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
        g.contact_id       as ghl_contact_id,
        g.first_name       as ghl_first_name,
        g.last_name        as ghl_last_name,
        g.email            as ghl_email,
        g.tags             as ghl_tags,
        g.created_at       as ghl_created_at,
        g.last_activity_at as ghl_last_activity_at,
        m.match_type       as ghl_match_type
    from wa
    left join matches m using (user_phone_hash, group_name)
    left join ghl g on m.ghl_contact_id = g.contact_id

),

-- GHL contacts with no WA member (anti-join via left join + is null)
ghl_only as (

    select
        null::text        as user_phone,
        null::text        as user_phone_hash,
        null::text        as group_name,
        null::timestamp   as joined_at,
        null::timestamp   as left_at,
        null::interval    as duration_in_group,
        null::text        as last_event_type,
        null::text        as status,
        null::text        as duration_category,
        g.contact_id      as ghl_contact_id,
        g.first_name      as ghl_first_name,
        g.last_name       as ghl_last_name,
        g.email           as ghl_email,
        g.tags            as ghl_tags,
        g.created_at      as ghl_created_at,
        g.last_activity_at as ghl_last_activity_at,
        null::text        as ghl_match_type
    from ghl g
    left join matches m on g.contact_id = m.ghl_contact_id
    where m.ghl_contact_id is null

)

select * from wa_enriched
union all
select * from ghl_only
