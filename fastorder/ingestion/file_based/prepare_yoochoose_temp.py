from pathlib import Path


RAW_FILE = Path(
    "data/raw/clickstream/"
    "yoochoose/extracted/yoochoose-clicks.dat"
)

TEMP_ROOT = Path(
    "data/tmp/yoochoose_prepare"
)

EXPECTED_ROWS = 33_003_944
EXPECTED_DATES = 183

PROGRESS_INTERVAL = 5_000_000


def prepare_yoochoose_temp(
    raw_file: Path,
    temp_root: Path,
) -> None:

    if not raw_file.exists():
        raise FileNotFoundError(
            f"Raw file không tồn tại: {raw_file}"
        )

    if temp_root.exists():
        existing_csv = list(
            temp_root.glob("*.csv")
        )

        if existing_csv:
            raise RuntimeError(
                "Temp preparation directory "
                "đã chứa CSV. "
                "Không overwrite tự động: "
                f"{temp_root}"
            )

    temp_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    total_rows = 0

    rows_by_date: dict[str, int] = {}
    file_handles = {}

    try:
        with raw_file.open(
            "r",
            encoding="utf-8",
        ) as source:

            for line_number, raw_line in enumerate(
                source,
                start=1,
            ):
                parts = (
                    raw_line
                    .rstrip("\r\n")
                    .split(",")
                )

                if len(parts) != 4:
                    raise RuntimeError(
                        "Malformed row tại "
                        f"line {line_number}"
                    )

                timestamp = parts[1]
                event_date = timestamp[:10]

                if event_date not in file_handles:

                    output_file = (
                        temp_root
                        / f"{event_date}.csv"
                    )

                    file_handles[event_date] = (
                        output_file.open(
                            "w",
                            encoding="utf-8",
                            newline="",
                        )
                    )

                    rows_by_date[event_date] = 0

                file_handles[event_date].write(
                    raw_line
                )

                rows_by_date[event_date] += 1
                total_rows += 1

                if (
                    total_rows
                    % PROGRESS_INTERVAL
                    == 0
                ):
                    print(
                        f"Processed: "
                        f"{total_rows:,} rows"
                    )

    finally:
        for handle in file_handles.values():
            handle.close()

    print()
    print("YOOCHOOSE TEMP PREPARATION")
    print(
        f"Input rows       : "
        f"{total_rows:,}"
    )
    print(
        f"Event dates      : "
        f"{len(rows_by_date):,}"
    )
    print(
        f"Temp CSV files   : "
        f"{len(file_handles):,}"
    )
    print(
        f"Temp root        : "
        f"{temp_root}"
    )

    if total_rows != EXPECTED_ROWS:
        raise RuntimeError(
            "Unexpected total row count."
        )

    if len(rows_by_date) != EXPECTED_DATES:
        raise RuntimeError(
            "Unexpected event-date count."
        )

    print()
    print(
        "YOOCHOOSE TEMP PREPARATION: PASS"
    )


if __name__ == "__main__":
    prepare_yoochoose_temp(
        raw_file=RAW_FILE,
        temp_root=TEMP_ROOT,
    )