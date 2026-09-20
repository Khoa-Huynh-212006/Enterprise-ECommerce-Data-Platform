with dates as (
    select
        generate_series(
            '{{ var("calendar_start_date") }}'::date,
            '{{ var("calendar_end_date") }}'::date,
            interval '1 day'
        )::date as date_day
)

select
    to_char(date_day, 'YYYYMMDD')::int as date_key,
    date_day,
    extract(year from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int as month,
    to_char(date_day, 'FMMonth') as month_name,
    extract(week from date_day)::int as week_of_year,
    extract(day from date_day)::int as day_of_month,
    extract(isodow from date_day)::int as day_of_week,
    to_char(date_day, 'FMDay') as day_name,
    (extract(isodow from date_day) in (6, 7)) as is_weekend
from dates