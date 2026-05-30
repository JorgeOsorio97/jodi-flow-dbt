# Project Context: JoDi Flow dbt

JoDi is an affiliate marketing company distributing offers via WhatsApp.
This dbt project transforms raw data loaded by the `jodi-flow` Python pipeline into analytics-ready tables in PostgreSQL.

## Database Connection

- **Profile:** `jodi_flow_dbt` in `~/.dbt/profiles.yml`
- **Host:** `jodiflow.ctigikugm0u3.us-east-2.rds.amazonaws.com`
- **Database:** `postgres` | **Schema:** `flow`
- **dbt version:** 1.9.6 | **Adapter:** postgres 1.9.0

> **SSH tunnel must be running before any `dbt` command.**

```bash
ssh -i /Users/jorge/Documents/Work/JoDi/keys/jodi-flow-proxy.pem \
    -l ec2-user 3.21.237.44 -p 22 -N -C -L \
    "5432:jodiflow.ctigikugm0u3.us-east-2.rds.amazonaws.com:5432"
```

## Running dbt

```bash
dbt run                    # build all models
dbt test                   # run all data quality tests
dbt run -s <model_name>    # build a single model
dbt run -s stg+            # build staging layer only
dbt run -s mart+           # build mart layer and downstream
```

## Model Layer Architecture

```
Sources (raw PostgreSQL tables)
  ├── public.raw_whatsapp_logs       → source('whatsapp', 'raw_whatsapp_logs')
  └── facebook_ads.ads_insights      → source('meta_ads', 'ads_insights')

Staging (views) — models/stg/
  ├── stg_whatsapp_logs              Adds: group_number, event_value, rolling_member_count
  └── stg_ads_insights               Filters: account=JoDi Promos Mexico, date >= 2025-01-01

Mart (tables) — models/mart/
  ├── mart_members                   One row per user+group; joined_at, left_at, status, duration_category
  ├── mart_groups                    One row per group; current/historic member counts, activity timestamps
  └── mart_ads_insights              Ads enriched with group_number attribution

Metrics (tables) — models/mart/
  └── metric_cohort_retention        Daily cohort retention: cohort_date × day_date × members_remaining + retention_pct
```

## Naming Conventions

| Prefix | Layer | Materialization |
|---|---|---|
| `stg_*` | Staging — thin views over raw sources | view |
| `mart_*` | Mart — dimension/fact tables for reporting | table |
| `metric_*` | Analytical outputs (aggregated metrics) | table |

Source YAML files are named `src_<source>.yaml` and live inside the model's subfolder.

## Source YAML Locations

- `models/stg/whatsapp/src_whatsapp.yaml` — 4 columns, `not_null` tests on all, `accepted_values` on `event_type`
- `models/stg/meta_ads/src_meta_ads.yaml` — minimal definition, no column tests yet

## Key SQL Patterns

**Group number extraction** (shared across all layers):
```sql
cast(substring(group_name from '\d+') as integer) as group_number
```

**Latest event per member+group** (used in `mart_members`):
```sql
row_number() over (
    partition by user_phone_hash, group_name
    order by timestamp desc
)
```

**Duration categories** (Spanish, sort-prefixed for Looker Studio ordering):
```
'1. Miembro activo'         -- still active (left_at is null)
'2. Duro más de un mes'     -- >= 1 month
'3. Duro menos de un mes'   -- < 1 month
'4. Duro menos de una semana' -- < 1 week
'5. Duro menos de un día'   -- < 1 day
```

**Day spine** (used in `metric_cohort_retention`):
```sql
generate_series(min_cohort_date, current_date, interval '1 day')::date
```

## Raw Source Schema

`raw_whatsapp_logs` (schema: `public`):

| Column | Type | Description |
|---|---|---|
| `timestamp` | TIMESTAMP | Event timestamp from WhatsApp export |
| `group_name` | TEXT | WhatsApp group name (derived from export filename) |
| `user_phone_hash` | TEXT | SHA-256 hash of phone number or WhatsApp nickname |
| `event_type` | TEXT | One of: `joined`, `left`, `added` |

Unique constraint on `(timestamp, group_name, user_phone_hash, event_type)`.
Loaded by `jodi-flow` via SSH tunnel using chunked 500-row INSERTs with `ON CONFLICT DO NOTHING`.

## Data Quality Tests

Currently defined on sources only:
- All `raw_whatsapp_logs` columns: `not_null`
- `event_type`: `accepted_values` → `['joined', 'left', 'added']`

No model-level tests defined yet.

## Empty Stub Directories

`analyses/`, `macros/`, `seeds/`, `snapshots/`, `tests/` — placeholder directories for future use.
