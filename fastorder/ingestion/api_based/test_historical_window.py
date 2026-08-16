from datetime import date, timedelta

from fastorder.ingestion.api_based.historical_window import (
    build_historical_windows,
)


end_date = date(
    2026,
    8,
    15,
)


windows = build_historical_windows(
    end_date=end_date,
    total_days=90,
    window_days=30,
)


for index, window in enumerate(
    windows,
    start=1,
):
    print(
        f"Window {index}: "
        f"{window.start_date} "
        f"-> {window.end_date}"
    )


assert len(windows) == 3


assert windows[0].start_date == date(
    2026,
    5,
    18,
)

assert windows[-1].end_date == date(
    2026,
    8,
    15,
)


for window in windows:

    days = (
        window.end_date
        - window.start_date
    ).days + 1

    assert days <= 30


for previous, current in zip(
    windows,
    windows[1:],
):

    assert (
        previous.end_date
        + timedelta(days=1)
        == current.start_date
    )


total_covered_days = sum(
    (
        window.end_date
        - window.start_date
    ).days + 1
    for window in windows
)


assert total_covered_days == 90


print(
    "\nHistorical window test: PASS"
)