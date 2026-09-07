from pathlib import Path


RAW_FILE = Path(
    "data/raw/clickstream/"
    "yoochoose/extracted/yoochoose-clicks.dat"
)


def check_date_order(
    file_path: Path,
) -> None:

    if not file_path.exists():
        raise FileNotFoundError(
            f"Raw file không tồn tại: {file_path}"
        )

    previous_date = None
    total_rows = 0
    date_transitions = 0

    with file_path.open(
        "r",
        encoding="utf-8",
    ) as file:

        for line_number, raw_line in enumerate(
            file,
            start=1,
        ):
            total_rows += 1

            parts = raw_line.rstrip("\r\n").split(",")

            if len(parts) != 4:
                raise RuntimeError(
                    f"Malformed row tại line {line_number}"
                )

            timestamp = parts[1]
            event_date = timestamp[:10]

            if previous_date is not None:
                if event_date < previous_date:
                    raise RuntimeError(
                        "Event date bị giảm tại "
                        f"line {line_number}: "
                        f"{previous_date} -> {event_date}"
                    )

                if event_date != previous_date:
                    date_transitions += 1

            previous_date = event_date

            if total_rows % 5_000_000 == 0:
                print(
                    f"Checked: {total_rows:,} rows"
                )

    print()
    print("YOOCHOOSE DATE ORDER CHECK")
    print(f"Total rows       : {total_rows:,}")
    print(f"Date transitions : {date_transitions:,}")
    print()
    print("YOOCHOOSE DATE ORDER: PASS")


if __name__ == "__main__":
    check_date_order(
        RAW_FILE
    )