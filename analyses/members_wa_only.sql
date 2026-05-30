-- WhatsApp members with no matching GHL contact
select
    user_phone,
    user_phone_hash,
    group_name,
    joined_at,
    left_at,
    duration_in_group,
    last_event_type,
    status,
    duration_category
from {{ ref('mart_members') }}
where user_phone_hash is not null
  and ghl_contact_id is null
