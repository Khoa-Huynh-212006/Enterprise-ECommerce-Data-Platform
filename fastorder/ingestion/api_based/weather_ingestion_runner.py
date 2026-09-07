from dataclasses import dataclass
from datetime import date, datetime, timezone

from botocore.client import BaseClient

from fastorder.ingestion.api_based.weather_api_client import (
    fetch_forecast,
    fetch_historical_forecast,
)

from fastorder.ingestion.api_based.weather_ingestion_metadata import (
    build_weather_ingestion_metadata,
)

from fastorder.ingestion.api_based.weather_bronze_writer import (
    build_weather_ingestion_root,
    write_weather_to_bronze,
    build_historical_weather_ingestion_root,
    write_historical_weather_to_bronze,
)

from fastorder.ingestion.api_based.weather_ingestion_state import (
    build_weather_ingestion_id,
    build_historical_weather_ingestion_id,
    success_marker_exists,
    write_success_marker,
)

from fastorder.ingestion.api_based.historical_window import (
    build_historical_windows,
)

from fastorder.ingestion.api_based.weather_config import (
    WAREHOUSE_WEATHER_LOCATIONS,
)


@dataclass(frozen=True)
class WeatherIngestionResult:
    status: str

    warehouse_id: str
    ingestion_id: str
    ingestion_root: str

    response_path: str | None
    metadata_path: str | None
    success_path: str | None


@dataclass(frozen=True)
class WeatherBatchIngestionResult:
    total: int
    committed: int
    skipped: int
    results: tuple[WeatherIngestionResult, ...]


def run_forecast_ingestion(
    *,
    minio_client: BaseClient,
    warehouse_id: str,
    latitude: float,
    longitude: float,
    run_id: str,
    logical_at: datetime,
) -> WeatherIngestionResult:

    if not warehouse_id:
        raise ValueError(
            "warehouse_id không được để trống"
        )

    if not run_id:
        raise ValueError(
            "run_id không được để trống"
        )

    if not isinstance(logical_at, datetime):
        raise ValueError(
            "logical_at phải là datetime"
        )

    if logical_at.tzinfo is None:
        raise ValueError(
            "logical_at phải timezone-aware"
        )

    api_type = "forecast"

    ingestion_id = build_weather_ingestion_id(
        api_type=api_type,
        warehouse_id=warehouse_id,
        run_id=run_id,
    )

    ingestion_root = build_weather_ingestion_root(
        api_type=api_type,
        warehouse_id=warehouse_id,
        ingestion_id=ingestion_id,
        logical_at=logical_at,
    )

    if success_marker_exists(
        minio_client=minio_client,
        ingestion_root=ingestion_root,
    ):

        print(
            "SKIP: Weather snapshot đã commit: "
            f"{warehouse_id}"
        )

        return WeatherIngestionResult(
            status="skipped",
            warehouse_id=warehouse_id,
            ingestion_id=ingestion_id,
            ingestion_root=ingestion_root,
            response_path=None,
            metadata_path=None,
            success_path=(
                f"{ingestion_root}/_SUCCESS"
            ),
        )

    requested_at = datetime.now(
        timezone.utc
    )

    api_result = fetch_forecast(
        latitude=latitude,
        longitude=longitude,
    )

    metadata = build_weather_ingestion_metadata(
        api_type=api_type,
        warehouse_id=warehouse_id,
        run_id=run_id,
        ingestion_id=ingestion_id,
        logical_at=logical_at,
        requested_at=requested_at,
        api_result=api_result,
    )

    response_path, metadata_path = (
        write_weather_to_bronze(
            minio_client=minio_client,
            api_result=api_result,
            metadata=metadata,
        )
    )

    success_path = write_success_marker(
        minio_client=minio_client,
        ingestion_root=ingestion_root,
    )

    print(
        "COMMITTED: Weather forecast: "
        f"{warehouse_id}"
    )

    return WeatherIngestionResult(
        status="committed",
        warehouse_id=warehouse_id,
        ingestion_id=ingestion_id,
        ingestion_root=ingestion_root,
        response_path=response_path,
        metadata_path=metadata_path,
        success_path=success_path,
    )


def run_all_forecast_ingestions(
    *,
    minio_client: BaseClient,
    run_id: str,
    logical_at: datetime,
) -> WeatherBatchIngestionResult:

    results = []

    committed = 0
    skipped = 0

    for warehouse in WAREHOUSE_WEATHER_LOCATIONS:

        print(
            "\nSTART: Weather forecast: "
            f"{warehouse.warehouse_id}"
        )

        result = run_forecast_ingestion(
            minio_client=minio_client,
            warehouse_id=warehouse.warehouse_id,
            latitude=warehouse.latitude,
            longitude=warehouse.longitude,
            run_id=run_id,
            logical_at=logical_at,
        )

        results.append(result)

        if result.status == "committed":
            committed += 1

        elif result.status == "skipped":
            skipped += 1

        else:
            raise RuntimeError(
                "Weather ingestion trả status "
                f"không hợp lệ: {result.status}"
            )

    return WeatherBatchIngestionResult(
        total=len(results),
        committed=committed,
        skipped=skipped,
        results=tuple(results),
    )


def run_historical_forecast_ingestion(
    *,
    minio_client: BaseClient,
    warehouse_id: str,
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
    run_id: str,
    logical_at: datetime,
) -> WeatherIngestionResult:

    if not warehouse_id:
        raise ValueError(
            "warehouse_id không được rỗng"
        )

    if not run_id:
        raise ValueError(
            "run_id không được rỗng"
        )

    if not isinstance(start_date, date):
        raise ValueError(
            "start_date phải là date"
        )

    if not isinstance(end_date, date):
        raise ValueError(
            "end_date phải là date"
        )

    if start_date > end_date:
        raise ValueError(
            "start_date không được lớn hơn end_date"
        )

    if not isinstance(logical_at, datetime):
        raise ValueError(
            "logical_at phải là datetime"
        )

    if logical_at.tzinfo is None:
        raise ValueError(
            "logical_at phải timezone-aware"
        )

    api_type = "historical_forecast"



    ingestion_id = (
        build_historical_weather_ingestion_id(
            warehouse_id=warehouse_id,
            start_date=start_date,
            end_date=end_date,
        )
    )



    ingestion_root = (
        build_historical_weather_ingestion_root(
            warehouse_id=warehouse_id,
            ingestion_id=ingestion_id,
            start_date=start_date,
            end_date=end_date,
        )
    )

    if success_marker_exists(
        minio_client=minio_client,
        ingestion_root=ingestion_root,
    ):

        print(
            "SKIP: Historical weather đã commit: "
            f"{warehouse_id} "
            f"{start_date} -> {end_date}"
        )

        return WeatherIngestionResult(
            status="skipped",
            warehouse_id=warehouse_id,
            ingestion_id=ingestion_id,
            ingestion_root=ingestion_root,
            response_path=None,
            metadata_path=None,
            success_path=(
                f"{ingestion_root}/_SUCCESS"
            ),
        )

    requested_at = datetime.now(
        timezone.utc
    )

    api_result = fetch_historical_forecast(
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
    )


    metadata = build_weather_ingestion_metadata(
        api_type=api_type,
        warehouse_id=warehouse_id,
        run_id=run_id,
        ingestion_id=ingestion_id,
        logical_at=logical_at,
        requested_at=requested_at,
        api_result=api_result,
    )

    response_path, metadata_path = (
        write_historical_weather_to_bronze(
            minio_client=minio_client,
            api_result=api_result,
            metadata=metadata,
            start_date=start_date,
            end_date=end_date,
        )
    )

    success_path = write_success_marker(
        minio_client=minio_client,
        ingestion_root=ingestion_root,
    )

    print(
        "COMMITTED: Historical weather: "
        f"{warehouse_id} "
        f"{start_date} -> {end_date}"
    )

    return WeatherIngestionResult(
        status="committed",
        warehouse_id=warehouse_id,
        ingestion_id=ingestion_id,
        ingestion_root=ingestion_root,
        response_path=response_path,
        metadata_path=metadata_path,
        success_path=success_path,
    )

def run_all_historical_forecast_ingestions(
    *,
    minio_client: BaseClient,
    end_date: date,
    run_id: str,
    logical_at: datetime,
    total_days: int = 90,
    window_days: int = 30,
) -> WeatherBatchIngestionResult:

    windows = build_historical_windows(
        end_date=end_date,
        total_days=total_days,
        window_days=window_days,
    )

    results = []

    committed = 0
    skipped = 0

    for window in windows:

        print(
            "\nHISTORICAL WINDOW: "
            f"{window.start_date} "
            f"-> {window.end_date}"
        )

        for warehouse in WAREHOUSE_WEATHER_LOCATIONS:

            print(
                "START: Historical weather: "
                f"{warehouse.warehouse_id} "
                f"{window.start_date} "
                f"-> {window.end_date}"
            )

            result = run_historical_forecast_ingestion(
                minio_client=minio_client,

                warehouse_id=warehouse.warehouse_id,
                latitude=warehouse.latitude,
                longitude=warehouse.longitude,

                start_date=window.start_date,
                end_date=window.end_date,

                run_id=run_id,
                logical_at=logical_at,
            )

            results.append(result)

            if result.status == "committed":
                committed += 1

            elif result.status == "skipped":
                skipped += 1

            else:
                raise RuntimeError(
                    "Historical weather ingestion "
                    "trả status không hợp lệ: "
                    f"{result.status}"
                )

    return WeatherBatchIngestionResult(
        total=len(results),
        committed=committed,
        skipped=skipped,
        results=tuple(results),
    )