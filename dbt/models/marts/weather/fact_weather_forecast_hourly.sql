select
    warehouse_id,
    ingestion_id,
    snapshot_at,
    retrieved_at,
    forecast_time,

    to_char(snapshot_at::date, 'YYYYMMDD')::int as snapshot_date_key,
    to_char(forecast_time::date, 'YYYYMMDD')::int as forecast_date_key,
    extract(hour from forecast_time)::int as forecast_hour,

    round(
        extract(epoch from (forecast_time - snapshot_at)) / 3600.0,
        2
    ) as forecast_horizon_hours,

    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    weather_code,

    requested_latitude,
    requested_longitude,
    response_latitude,
    response_longitude

from {{ source('staging_weather', 'weather_forecast_hourly') }}