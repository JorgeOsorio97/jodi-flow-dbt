-- Coverage summary: how members are distributed across WA and GHL
select
    count(case when ghl_match_type is not null                    then 1 end) as in_both,
    count(case when user_phone_hash is null                       then 1 end) as ghl_only,
    count(case when user_phone_hash is not null
                and ghl_contact_id is null                        then 1 end) as wa_only,
    count(*)                                                                   as total,
    round(count(case when ghl_match_type is not null then 1 end)::numeric
          / nullif(count(*), 0) * 100, 1)                                      as pct_in_both,
    round(count(case when user_phone_hash is null then 1 end)::numeric
          / nullif(count(*), 0) * 100, 1)                                      as pct_ghl_only,
    round(count(case when user_phone_hash is not null
                      and ghl_contact_id is null then 1 end)::numeric
          / nullif(count(*), 0) * 100, 1)                                      as pct_wa_only
from {{ ref('mart_members') }}
