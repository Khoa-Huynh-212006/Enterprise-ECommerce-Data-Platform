{{ config(materialized='table') }}

with clicks as (
    select
        session_id,
        event_timestamp,
        event_date,
        item_id,
        category
    from {{ source('staging_clickstream', 'yoochoose_clicks') }}
),

sessions as (
    select
        session_id,
        min(event_timestamp) as session_start_at,
        max(event_timestamp) as session_end_at,
        min(event_date) as session_start_date,
        max(event_date) as session_end_date,

        count(*) as event_count,
        count(distinct item_id) as distinct_item_count,
        count(distinct category) as distinct_category_count,

        extract(
            epoch from max(event_timestamp) - min(event_timestamp)
        )::bigint as session_duration_seconds

    from clicks
    group by session_id
)

select *
from sessions