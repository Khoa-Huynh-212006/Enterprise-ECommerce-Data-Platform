import io
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from fastorder.db.connection import (
    get_engine,
)

from fastorder.storage.adls_client import (
    get_bronze_file_system_client,
)

from fastorder.ingestion.incremental.table_config import (
    get_table_config,
)

from fastorder.ingestion.incremental.table_extractor import (
    get_upper_watermark,
)

from fastorder.ingestion.incremental.checkpoint_manager import (
    load_checkpoint,
)


TABLES = (
    "orders",
    "customers",
    "warehouses",
    "product_category_name_translation",
    "sellers",
    "products",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "inventory",
)


METADATA_COLUMNS = (
    "_ingestion_id",
    "_ingested_at",
    "_source_table",
    "_source_updated_at",
    "_ingestion_method",
)


CHECKPOINT_ROOT = Path(
    "/opt/airflow/state/checkpoints"
)

PENDING_ROOT = Path(
    "/opt/airflow/state/pending"
)


TIMESTAMP_FORMAT = (
    "%Y-%m-%dT%H:%M:%S.%f"
)


def normalize_scalar(value):

    if hasattr(value, "item"):
        return value.item()

    return value


def build_watermark_from_row(
    row,
    config,
):

    timestamp = pd.Timestamp(
        row[config.watermark_column]
    ).to_pydatetime()

    watermark = {
        config.watermark_column:
            timestamp.strftime(
                TIMESTAMP_FORMAT
            )
    }

    for column in (
        config.primary_key_columns
    ):
        watermark[column] = (
            normalize_scalar(
                row[column]
            )
        )

    return watermark


def validate_bronze_path(
    path,
    table_name,
):

    parts = path.split("/")

    if len(parts) != 4:
        raise AssertionError(
            f"Path sai cấu trúc: {path}"
        )

    if parts[0] != table_name:
        raise AssertionError(
            f"Source table trong path sai: "
            f"{path}"
        )

    if not parts[1].startswith(
        "ingestion_date="
    ):
        raise AssertionError(
            f"Thiếu ingestion_date: {path}"
        )

    if not parts[2].startswith(
        "extraction_id="
    ):
        raise AssertionError(
            f"Thiếu extraction_id: {path}"
        )

    if parts[3] != "part-000.parquet":
        raise AssertionError(
            f"Tên Parquet sai: {path}"
        )

    return parts[2].split(
        "=",
        1,
    )[1]


def validate_table(
    conn,
    fs_client,
    table_name,
):

    config = get_table_config(
        table_name
    )

    checkpoint_path = (
        CHECKPOINT_ROOT
        / f"{table_name}_checkpoint.json"
    )

    pending_path = (
        PENDING_ROOT
        / f"{table_name}_pending_batch.json"
    )


    source_count = conn.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            """
        )
    ).scalar_one()

    source_upper = get_upper_watermark(
        conn=conn,
        config=config,
    )

    assert source_count > 0

    assert source_upper is not None



    assert checkpoint_path.exists(), (
        f"{table_name}: "
        "checkpoint không tồn tại."
    )

    checkpoint = load_checkpoint(
        checkpoint_path,
        table_name,
    )

    assert (
        checkpoint["watermark"]
        == source_upper
    ), (
        f"{table_name}: "
        "checkpoint != source upper.\n"
        f"Checkpoint: "
        f"{checkpoint['watermark']}\n"
        f"Source upper: "
        f"{source_upper}"
    )

    assert not pending_path.exists(), (
        f"{table_name}: "
        "pending context vẫn còn."
    )


    bronze_paths = [
        path.name
        for path in fs_client.get_paths(
            path=table_name,
            recursive=True,
        )
        if (
            not path.is_directory
            and path.name.endswith(
                "/part-000.parquet"
            )
        )
    ]

    assert bronze_paths, (
        f"{table_name}: "
        "không tìm thấy Bronze Parquet."
    )

    production_files = 0
    bronze_versions = 0

    distinct_primary_keys = set()

    bronze_max_key = None
    bronze_max_watermark = None

    cursor_columns = (
        config.watermark_column,
        *config.primary_key_columns,
    )



    for path in bronze_paths:

        extraction_id = (
            validate_bronze_path(
                path,
                table_name,
            )
        )

        # Test artifacts không thuộc
        # production coverage.
        if extraction_id.startswith(
            "test_"
        ):
            continue

        production_files += 1

        file_client = (
            fs_client.get_file_client(
                path
            )
        )

        raw = (
            file_client
            .download_file()
            .readall()
        )

        assert len(raw) > 0, (
            f"{table_name}: "
            f"file rỗng: {path}"
        )

        df = pd.read_parquet(
            io.BytesIO(raw)
        )

        assert not df.empty, (
            f"{table_name}: "
            f"Parquet không có row: {path}"
        )

        expected_columns = {
            *config.select_columns,
            *METADATA_COLUMNS,
        }

        missing_columns = (
            expected_columns
            - set(df.columns)
        )

        assert not missing_columns, (
            f"{table_name}: "
            f"thiếu columns "
            f"{missing_columns}"
        )

        # Metadata contract
        assert (
            df["_source_table"]
            .eq(table_name)
            .all()
        )

        assert (
            df["_ingestion_id"]
            .eq(extraction_id)
            .all()
        )

        assert (
            df["_ingestion_method"]
            .eq(
                "timestamp_incremental"
            )
            .all()
        )

        assert (
            df["_ingested_at"]
            .notna()
            .all()
        )

        assert (
            df["_source_updated_at"]
            .notna()
            .all()
        )

        # Source updated_at phải khớp
        # lineage metadata.
        source_updated = pd.to_datetime(
            df[config.watermark_column]
        )

        metadata_updated = pd.to_datetime(
            df["_source_updated_at"]
        )

        assert source_updated.equals(
            metadata_updated
        )

        # PK không được null.
        for pk_column in (
            config.primary_key_columns
        ):
            assert (
                df[pk_column]
                .notna()
                .all()
            )

        bronze_versions += len(df)


        pk_frame = (
            df[
                list(
                    config.primary_key_columns
                )
            ]
            .drop_duplicates()
        )

        for pk_tuple in (
            pk_frame.itertuples(
                index=False,
                name=None,
            )
        ):
            distinct_primary_keys.add(
                tuple(
                    normalize_scalar(v)
                    for v in pk_tuple
                )
            )

        ordered = df.sort_values(
            list(cursor_columns)
        )

        last_row = ordered.iloc[-1]

        candidate_watermark = (
            build_watermark_from_row(
                last_row,
                config,
            )
        )

        timestamp = pd.Timestamp(
            last_row[
                config.watermark_column
            ]
        ).to_pydatetime()

        candidate_key = (
            timestamp,
            *(
                normalize_scalar(
                    last_row[column]
                )
                for column
                in config.primary_key_columns
            ),
        )

        if (
            bronze_max_key is None
            or candidate_key
            > bronze_max_key
        ):
            bronze_max_key = (
                candidate_key
            )

            bronze_max_watermark = (
                candidate_watermark
            )


    assert production_files > 0, (
        f"{table_name}: "
        "chỉ tìm thấy test files."
    )

    assert (
        len(distinct_primary_keys)
        == source_count
    ), (
        f"{table_name}: "
        "business PK coverage không khớp.\n"
        f"Source rows: {source_count}\n"
        f"Bronze distinct PK: "
        f"{len(distinct_primary_keys)}"
    )

    assert (
        bronze_max_watermark
        == source_upper
    ), (
        f"{table_name}: "
        "Bronze max cursor != "
        "source upper.\n"
        f"Bronze max: "
        f"{bronze_max_watermark}\n"
        f"Source upper: "
        f"{source_upper}"
    )

    return {
        "table": table_name,
        "source_rows": source_count,
        "bronze_distinct_pk":
            len(distinct_primary_keys),
        "bronze_versions":
            bronze_versions,
        "production_files":
            production_files,
        "checkpoint_matches":
            True,
        "pending_clean":
            True,
        "bronze_reaches_upper":
            True,
    }


def main():

    engine = get_engine()

    fs_client = (
        get_bronze_file_system_client()
    )

    results = []
    failures = []

    with engine.connect() as conn:

        for table_name in TABLES:

            print(
                "\n"
                + "=" * 70
            )

            print(
                f"VALIDATING: "
                f"{table_name}"
            )

            try:

                result = validate_table(
                    conn=conn,
                    fs_client=fs_client,
                    table_name=table_name,
                )

                results.append(
                    result
                )

                print(
                    f"[PASS] "
                    f"{table_name}"
                )

                print(
                    f"  Source rows       : "
                    f"{result['source_rows']}"
                )

                print(
                    f"  Bronze distinct PK: "
                    f"{result['bronze_distinct_pk']}"
                )

                print(
                    f"  Bronze versions   : "
                    f"{result['bronze_versions']}"
                )

                print(
                    f"  Production files  : "
                    f"{result['production_files']}"
                )

            except Exception as error:

                failures.append(
                    (
                        table_name,
                        error,
                    )
                )

                print(
                    f"[FAIL] "
                    f"{table_name}"
                )

                print(
                    f"  {type(error).__name__}: "
                    f"{error}"
                )

    print(
        "\n"
        + "=" * 70
    )

    print(
        "OPERATIONAL BRONZE "
        "E2E SUMMARY"
    )

    print(
        "=" * 70
    )

    for result in results:

        print(
            f"[PASS] "
            f"{result['table']}"
        )

    for table_name, error in failures:

        print(
            f"[FAIL] "
            f"{table_name}: "
            f"{error}"
        )

    if failures:

        raise RuntimeError(
            "Operational Bronze "
            "E2E validation FAILED."
        )

    assert (
        len(results)
        == len(TABLES)
    )

    print(
        "\n"
        "OPERATIONAL BRONZE E2E: "
        "PERFECT PASS "
        f"({len(results)}/{len(TABLES)})"
    )


if __name__ == "__main__":
    main()