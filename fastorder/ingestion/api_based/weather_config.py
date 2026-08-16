from dataclasses import dataclass


@dataclass(frozen=True)
class WarehouseWeatherLocation:
    warehouse_id: str
    latitude: float
    longitude: float


WAREHOUSE_WEATHER_LOCATIONS = (
    WarehouseWeatherLocation(
        warehouse_id="WH_HN",
        latitude=21.0285,
        longitude=105.8542,
    ),

    WarehouseWeatherLocation(
        warehouse_id="WH_HP",
        latitude=20.8449,
        longitude=106.6881,
    ),

    WarehouseWeatherLocation(
        warehouse_id="WH_DN",
        latitude=16.0544,
        longitude=108.2022,
    ),

    WarehouseWeatherLocation(
        warehouse_id="WH_HCM",
        latitude=10.8231,
        longitude=106.6297,
    ),

    WarehouseWeatherLocation(
        warehouse_id="WH_CT",
        latitude=10.0452,
        longitude=105.7469,
    )
)