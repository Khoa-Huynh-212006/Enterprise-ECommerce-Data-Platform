from dataclasses import dataclass
from datetime import date, timedelta


@dataclass(frozen=True)
class HistoricalWindow:
    start_date: date
    end_date: date


def build_historical_windows(
    *,
    end_date: date,
    total_days: int = 90,
    window_days: int = 30,
) -> tuple[HistoricalWindow, ...]:

    if not isinstance(end_date, date):
        raise ValueError(
            "end_date phải là date"
        )

    if total_days <= 0:
        raise ValueError(
            "total_days phải > 0"
        )

    if window_days <= 0:
        raise ValueError(
            "window_days phải > 0"
        )

    bootstrap_start = (
        end_date - timedelta(days=total_days - 1)
    )

    windows = []

    current_start = bootstrap_start

    while current_start <= end_date:

        current_end = min(
            current_start
            + timedelta(days=window_days - 1),
            end_date,
        )

        windows.append(
            HistoricalWindow(
                start_date=current_start,
                end_date=current_end,
            )
        )

        current_start = (
            current_end
            + timedelta(days=1)
        )

    return tuple(windows)