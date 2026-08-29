from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IncrementalTableConfig:
    table_name: str

    primary_key_columns: tuple[str, ...]

    initial_primary_key_values: tuple[Any, ...]

    watermark_column: str

    select_columns: tuple[str, ...]

    def __post_init__(self) -> None:

        if not self.table_name:
            raise ValueError(
                "table_name không được để trống."
            )

        if not self.primary_key_columns:
            raise ValueError(
                "primary_key_columns không được để trống."
            )

        if (
            len(self.primary_key_columns)
            != len(self.initial_primary_key_values)
        ):
            raise ValueError(
                "Mỗi primary key column phải có "
                "một initial value tương ứng."
            )

        if not self.watermark_column:
            raise ValueError(
                "watermark_column không được để trống."
            )

        if not self.select_columns:
            raise ValueError(
                "select_columns không được để trống."
            )

        missing_primary_keys = [
            column
            for column in self.primary_key_columns
            if column not in self.select_columns
        ]

        if missing_primary_keys:
            raise ValueError(
                "Primary key columns không tồn tại trong "
                f"select_columns: {missing_primary_keys}"
            )

        if self.watermark_column not in self.select_columns:
            raise ValueError(
                f"Watermark column '{self.watermark_column}' "
                "không tồn tại trong select_columns."
            )


ORDERS_CONFIG = IncrementalTableConfig(
    table_name="orders",

    primary_key_columns=(
        "order_id",
    ),

    initial_primary_key_values=(
        "",
    ),

    watermark_column="updated_at",

    select_columns=(
        "order_id",
        "customer_id",
        "warehouse_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "source_system",
        "created_at",
        "updated_at",
    ),
)


TABLE_CONFIGS = {
    ORDERS_CONFIG.table_name: ORDERS_CONFIG,
}


def get_table_config(
    table_name: str,
) -> IncrementalTableConfig:

    try:
        return TABLE_CONFIGS[table_name]

    except KeyError as error:
        raise ValueError(
            f"Chưa cấu hình incremental ingestion "
            f"cho table '{table_name}'."
        ) from error

if __name__ == "__main__":

    print("Testing ORDERS_CONFIG...")

    assert ORDERS_CONFIG.table_name == "orders"

    assert ORDERS_CONFIG.primary_key_columns == (
        "order_id",
    )

    assert ORDERS_CONFIG.watermark_column == (
        "updated_at"
    )

    assert "order_id" in ORDERS_CONFIG.select_columns
    assert "updated_at" in ORDERS_CONFIG.select_columns

    print("ORDERS_CONFIG: PASS")