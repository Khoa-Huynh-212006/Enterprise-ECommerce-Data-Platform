select
    warehouse_id,
    weather_time,

    to_char(weather_time::date, 'YYYYMMDD')::int as weather_date_key,
    extract(hour from weather_time)::int as weather_hour,

    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    weather_code,

    ingestion_id,
    retrieved_at,
    window_start,
    window_end,

    requested_latitude,
    requested_longitude,
    response_latitude,
    response_longitude

from {{ source('staging_weather', 'weather_historical_forecast_hourly') }}