-- GHL contacts with no matching WhatsApp member
select
    ghl_contact_id,
    ghl_first_name,
    ghl_last_name,
    ghl_email,
    ghl_tags,
    ghl_created_at,
    ghl_last_activity_at
from {{ ref('mart_members') }}
where user_phone_hash is null
