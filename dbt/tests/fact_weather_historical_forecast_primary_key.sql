select warehouse_id, weather_time
from {{ ref('fact_weather_historical_forecast_hourly') }}
group by warehouse_id, weather_time
having count(*) > 1