select warehouse_id, weather_time
from {{ source('staging_weather', 'weather_historical_forecast_hourly') }}
group by warehouse_id, weather_time
having count(*) > 1