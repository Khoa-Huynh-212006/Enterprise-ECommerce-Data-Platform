select
    session_id,
    session_start_at,
    session_end_at,

    to_char(session_start_date, 'YYYYMMDD')::int as start_date_key,
    to_char(session_end_date, 'YYYYMMDD')::int as end_date_key,

    event_count,
    session_duration_seconds,

    (session_start_date <> session_end_date) as is_cross_date_session,

    1 as session_count

from {{ ref('int_clickstream_sessions') }}