{{
    config(
        materialized='table',
        post_hook=[
            "create index if not exists idx_wa_ghl_matches_wa on {{ this }} (user_phone_hash, group_name)",
            "create index if not exists idx_wa_ghl_matches_contact on {{ this }} (ghl_contact_id)",
        ]
    )
}}

-- Phone matches: one INNER JOIN, uses index on normalized_phone
select
    wa.user_phone_hash,
    wa.group_name,
    g.contact_id as ghl_contact_id,
    'phone'      as match_type
from {{ ref('int_wa_members') }} wa
inner join {{ ref('stg_ghl_contacts') }} g
    on wa.normalized_phone = g.normalized_phone
where wa.normalized_phone is not null

union all

-- Name matches: placeholder entries, two separate joins (one per column) to allow index use
select
    wa.user_phone_hash,
    wa.group_name,
    coalesce(gf.contact_id, gr.contact_id) as ghl_contact_id,
    'name'                                  as match_type
from {{ ref('int_wa_members') }} wa
left join {{ ref('stg_ghl_contacts') }} gf
    on wa.placeholder_name = gf.normalized_name_fwd
    and gf.is_unique_name
left join {{ ref('stg_ghl_contacts') }} gr
    on gf.contact_id is null
    and wa.placeholder_name = gr.normalized_name_rev
    and gr.is_unique_name
where wa.placeholder_name is not null
  and coalesce(gf.contact_id, gr.contact_id) is not null
