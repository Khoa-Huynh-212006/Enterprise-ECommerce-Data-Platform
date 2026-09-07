with sessions as (
    select
        start_date_key as date_key,
        count(*) as sessions_started,
        sum(event_count) as session_event_count,
        avg(session_duration_seconds) as avg_session_duration_seconds
    from {{ ref('fact_clickstream_sessions') }}
    group by start_date_key
),

daily_events as (
    select
        event_date,
        to_char(event_date, 'YYYYMMDD')::int as date_key,
        count(*) as event_count,
        count(distinct session_id) as active_session_count,
        count(distinct item_id) as distinct_item_count,
        count(distinct category) as distinct_category_count
    from {{ source('staging_clickstream', 'yoochoose_clicks') }}
    group by event_date
)

select
    e.date_key,
    e.event_date,
    s.sessions_started,
    e.active_session_count,
    e.event_count,
    e.distinct_item_count,
    e.distinct_category_count,
    s.avg_session_duration_seconds

from daily_events e
left join sessions s
    on e.date_key = s.date_key