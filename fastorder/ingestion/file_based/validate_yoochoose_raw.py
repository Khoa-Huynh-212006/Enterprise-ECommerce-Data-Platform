from datetime import date
from pathlib import Path


# RAW_FILE = Path(
#     "/opt/airflow/data/raw/clickstream/yoochoose/extracted/yoochoose-clicks.dat"
# )

RAW_FILE = Path(
    "data/raw/clickstream/"
    "yoochoose/extracted/yoochoose-clicks.dat"
)

def validate_yoochoose_raw(
    file_path: Path,
) -> None:

    if not file_path.exists():
        raise FileNotFoundError(
            f"Raw file không tồn tại: {file_path}"
        )

    total_rows = 0
    malformed_rows = 0 # dòng bị lỗi

    event_dates: set[str] = set()

    min_timestamp = None
    max_timestamp = None

    malformed_examples: list[str] = []

    with file_path.open(
        "r",
        encoding="utf-8",
    ) as file:

        for line_number, raw_line in enumerate(
            file,
            start=1,
        ):
            total_rows += 1

            line = raw_line.rstrip("\r\n")

            parts = line.split(",")

            if len(parts) != 4:
                malformed_rows += 1

                if len(malformed_examples) < 10:
                    malformed_examples.append(
                        f"line={line_number}: {line}"
                    )

                continue

            (
                session_id,
                timestamp,
                item_id,
                category,
            ) = parts

            if (
                not session_id.isdigit()
                or not item_id.isdigit()
                or not timestamp
                or not category
            ):
                malformed_rows += 1

                if len(malformed_examples) < 10:
                    malformed_examples.append(
                        f"line={line_number}: {line}"
                    )

                continue

            if (
                len(timestamp) < 10
                or timestamp[4] != "-"
                or timestamp[7] != "-"
            ):
                malformed_rows += 1

                if len(malformed_examples) < 10:
                    malformed_examples.append(
                        f"line={line_number}: {line}"
                    )

                continue

            event_date = timestamp[:10]

            try:
                date.fromisoformat(event_date)
            except ValueError:
                malformed_rows += 1

                if len(malformed_examples) < 10:
                    malformed_examples.append(
                        f"line={line_number}: {line}"
                    )

                continue

            event_dates.add(event_date)

            if (
                min_timestamp is None
                or timestamp < min_timestamp
            ):
                min_timestamp = timestamp

            if (
                max_timestamp is None
                or timestamp > max_timestamp
            ):
                max_timestamp = timestamp

    print()
    print("YOOCHOOSE RAW VALIDATION")
    print(f"File            : {file_path}")
    print(f"Total rows      : {total_rows:,}")
    print(f"Malformed rows  : {malformed_rows:,}")
    print(f"Event dates     : {len(event_dates):,}")
    print(f"Min timestamp   : {min_timestamp}")
    print(f"Max timestamp   : {max_timestamp}")

    if malformed_examples:
        print()
        print("Malformed examples:")

        for example in malformed_examples:
            print(example)

    print()

    if malformed_rows != 0:
        raise RuntimeError(
            "YOOCHOOSE raw validation FAILED."
        )

    print("YOOCHOOSE RAW VALIDATION: PASS")


if __name__ == "__main__":
    validate_yoochoose_raw(
        RAW_FILE
    )