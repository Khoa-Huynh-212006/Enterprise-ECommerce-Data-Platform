select warehouse_id, ingestion_id, forecast_time
from {{ source('staging_weather', 'weather_forecast_hourly') }}
group by warehouse_id, ingestion_id, forecast_time
having count(*) > 1