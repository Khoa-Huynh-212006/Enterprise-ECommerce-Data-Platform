import re

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import Connection, text

from fastorder.ingestion.incremental.table_config import (
    IncrementalTableConfig,
)


TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def _format_timestamp(
    value: datetime,
) -> str:

    return value.strftime(
        TIMESTAMP_FORMAT
    )


def _parse_timestamp(
    value: str,
) -> datetime:

    try:
        return datetime.strptime(
            value,
            TIMESTAMP_FORMAT,
        )

    except ValueError as error:
        raise ValueError(
            f"Timestamp không hợp lệ: '{value}'. "
            "Kỳ vọng "
            "YYYY-MM-DDTHH:MM:SS.ffffff."
        ) from error


_IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*$"
)


def _validate_identifier(
    identifier: str,
) -> None:

    if not _IDENTIFIER_PATTERN.fullmatch(
        identifier
    ):
        raise ValueError(
            f"SQL identifier không hợp lệ: "
            f"'{identifier}'."
        )

def _validate_config_identifiers(
    config: IncrementalTableConfig,
) -> None:

    _validate_identifier(
        config.table_name
    )

    _validate_identifier(
        config.watermark_column
    )

    for column in (
        *config.primary_key_columns,
        *config.select_columns,
    ):
        _validate_identifier(column)


def _validate_watermark(
    watermark: dict[str, Any],
    config: IncrementalTableConfig,
) -> None:

    if not isinstance(
        watermark,
        dict,
    ):
        raise ValueError(
            "Watermark phải là dictionary."
        )

    if (
        config.watermark_column
        not in watermark
    ):
        raise ValueError(
            f"Watermark thiếu column "
            f"'{config.watermark_column}'."
        )

    _parse_timestamp(
        watermark[
            config.watermark_column
        ]
    )

    for column in (
        config.primary_key_columns
    ):
        if column not in watermark:
            raise ValueError(
                f"Watermark thiếu primary key "
                f"'{column}'."
            )

def get_upper_watermark(
    conn: Connection,
    config: IncrementalTableConfig,
) -> Optional[dict[str, Any]]:

    _validate_config_identifiers(
        config
    )

    cursor_columns = (
        config.watermark_column,
        *config.primary_key_columns,
    )

    select_clause = ", ".join(
        cursor_columns
    )

    order_clause = ", ".join(
        f"{column} DESC"
        for column in cursor_columns
    )

    query = text(
        f"""
        SELECT
            {select_clause}
        FROM {config.table_name}
        ORDER BY
            {order_clause}
        LIMIT 1
        """
    )

    result = conn.execute(
        query
    ).fetchone()

    if result is None:
        return None

    row = result._mapping

    watermark = {
        config.watermark_column:
            _format_timestamp(
                row[
                    config.watermark_column
                ]
            )
    }

    for column in (
        config.primary_key_columns
    ):
        watermark[column] = row[column]

    return watermark


def extract_table_batch(
    conn: Connection,
    config: IncrementalTableConfig,
    lower_watermark: dict[str, Any],
    upper_watermark: dict[str, Any],
    batch_size: int = 1000,
) -> tuple[
    list[dict[str, Any]],
    dict[str, Any],
]:

    if (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or batch_size <= 0
    ):
        raise ValueError(
            "batch_size phải là số nguyên > 0."
        )

    _validate_config_identifiers(
        config
    )

    _validate_watermark(
        lower_watermark,
        config,
    )

    _validate_watermark(
        upper_watermark,
        config,
    )


    cursor_columns = (
        config.watermark_column,
        *config.primary_key_columns,
    )

    lower_position = (
        _parse_timestamp(
            lower_watermark[
                config.watermark_column
            ]
        ),
        *(
            lower_watermark[column]
            for column
            in config.primary_key_columns
        ),
    )

    upper_position = (
        _parse_timestamp(
            upper_watermark[
                config.watermark_column
            ]
        ),
        *(
            upper_watermark[column]
            for column
            in config.primary_key_columns
        ),
    )

    if lower_position > upper_position:
        raise ValueError(
            "Lower watermark không được "
            "lớn hơn upper watermark."
        )

    if lower_position == upper_position:
        return [], lower_watermark

    select_clause = ",\n".join(
        f"            {column}"
        for column
        in config.select_columns
    )

    cursor_clause = ", ".join(
        cursor_columns
    )

    lower_param_names = [
        "lower_watermark",
        *[
            f"lower_pk_{index}"
            for index, _
            in enumerate(
                config.primary_key_columns
            )
        ],
    ]

    upper_param_names = [
        "upper_watermark",
        *[
            f"upper_pk_{index}"
            for index, _
            in enumerate(
                config.primary_key_columns
            )
        ],
    ]

    lower_parameter_clause = ", ".join(
        f":{name}"
        for name in lower_param_names
    )

    upper_parameter_clause = ", ".join(
        f":{name}"
        for name in upper_param_names
    )

    order_clause = ", ".join(
        f"{column} ASC"
        for column in cursor_columns
    )

    query = text(
        f"""
            SELECT
    {select_clause}
            FROM {config.table_name}
            WHERE
                ({cursor_clause})
                >
                ({lower_parameter_clause})
            AND
                ({cursor_clause})
                <=
                ({upper_parameter_clause})
            ORDER BY
                {order_clause}
            LIMIT :batch_size
        """
    )


    params = {
        "lower_watermark":
            lower_position[0],

        "upper_watermark":
            upper_position[0],

        "batch_size":
            batch_size,
    }

    for index, column in enumerate(
        config.primary_key_columns
    ):

        params[
            f"lower_pk_{index}"
        ] = lower_watermark[column]

        params[
            f"upper_pk_{index}"
        ] = upper_watermark[column]

    results = conn.execute(
        query,
        params,
    ).fetchall()

    if not results:
        return [], lower_watermark

    records = [
        dict(row._mapping)
        for row in results
    ]

    last_record = results[-1]._mapping

    next_watermark = {
        config.watermark_column:
            _format_timestamp(
                last_record[
                    config.watermark_column
                ]
            )
    }

    for column in (
        config.primary_key_columns
    ):
        next_watermark[column] = (
            last_record[column]
        )

    return (
        records,
        next_watermark,
    )