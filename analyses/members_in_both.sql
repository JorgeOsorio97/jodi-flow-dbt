-- Members present in both WhatsApp and GoHighLevel (matched by phone or name)
select
    user_phone,
    user_phone_hash,
    group_name,
    joined_at,
    left_at,
    duration_in_group,
    last_event_type,
    status,
    duration_category,
    ghl_contact_id,
    ghl_first_name,
    ghl_last_name,
    ghl_email,
    ghl_tags,
    ghl_created_at,
    ghl_last_activity_at,
    ghl_match_type
from {{ ref('mart_members') }}
where ghl_match_type is not null
