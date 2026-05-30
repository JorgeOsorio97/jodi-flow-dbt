{{
    config(
        materialized='table',
        post_hook=[
            "create index if not exists idx_ghl_contacts_phone on {{ this }} (normalized_phone)",
            "create index if not exists idx_ghl_contacts_name_fwd on {{ this }} (normalized_name_fwd)",
            "create index if not exists idx_ghl_contacts_name_rev on {{ this }} (normalized_name_rev)",
        ]
    )
}}

with raw as (

    select
        "Contact_Id"    as contact_id,
        "First_Name"    as first_name,
        "Last_Name"     as last_name,
        "Phone"         as phone,
        "Email"         as email,
        "Business_Name" as business_name,
        "Created"       as created_at,
        "Last_Activity" as last_activity_at,
        "Tags"          as tags,
        {{ normalize_phone('"Phone"') }}                             as normalized_phone,
        {{ normalize_name('"First_Name"', '"Last_Name"', 'fwd') }}  as normalized_name_fwd,
        {{ normalize_name('"First_Name"', '"Last_Name"', 'rev') }}  as normalized_name_rev,
        row_number() over (
            partition by {{ normalize_phone('"Phone"') }}
            order by "Last_Activity" desc nulls last
        ) as phone_rn
    from {{ source('gohighlevel', 'Contacts') }}

),

phone_deduped as (

    select * from raw
    where phone_rn = 1 or normalized_phone is null

),

name_counts as (

    select
        normalized_name_fwd,
        count(*) as cnt
    from phone_deduped
    where length(normalized_name_fwd) >= 8
    group by 1

)

select
    pd.contact_id,
    pd.first_name,
    pd.last_name,
    pd.phone,
    pd.email,
    pd.business_name,
    pd.created_at,
    pd.last_activity_at,
    pd.tags,
    pd.normalized_phone,
    pd.normalized_name_fwd,
    pd.normalized_name_rev,
    coalesce(nc.cnt = 1, false) as is_unique_name
from phone_deduped pd
left join name_counts nc on pd.normalized_name_fwd = nc.normalized_name_fwd
