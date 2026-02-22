{{
    config(
        materialized='table'
    )
}}

with members as (

    select
        user_phone_hash,
        group_name,
        joined_at::date as cohort_date,
        left_at::date   as left_date,
        status
    from {{ ref('mart_members') }}
    where joined_at is not null

),

cohort_sizes as (

    select
        cohort_date,
        count(*) as cohort_size
    from members
    group by cohort_date

),

day_spine as (

    select
        generate_series(
            (select min(cohort_date) from members),
            current_date,
            interval '1 day'
        )::date as day_date

),

cohort_days as (

    select
        c.cohort_date,
        d.day_date,
        (d.day_date - c.cohort_date) as days_since_join
    from cohort_sizes c
    cross join day_spine d
    where d.day_date >= c.cohort_date

),

members_remaining as (

    select
        cd.cohort_date,
        cd.day_date,
        cd.days_since_join,
        cs.cohort_size,
        cs.cohort_size - count(
            case when m.left_date is not null and m.left_date <= cd.day_date then 1 end
        ) as members_remaining
    from cohort_days cd
    inner join cohort_sizes cs
        on cd.cohort_date = cs.cohort_date
    inner join members m
        on m.cohort_date = cd.cohort_date
    group by
        cd.cohort_date,
        cd.day_date,
        cd.days_since_join,
        cs.cohort_size

)

select
    cohort_date,
    day_date,
    days_since_join,
    cohort_size,
    members_remaining,
    round(
        members_remaining::numeric / nullif(cohort_size, 0) * 100, 1
    ) as retention_pct
from members_remaining
order by cohort_date, days_since_join
