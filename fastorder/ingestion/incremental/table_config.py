from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IncrementalTableConfig:
    table_name: str

    primary_key_columns: tuple[str, ...]

    initial_primary_key_values: tuple[Any, ...]

    watermark_column: str

    select_columns: tuple[str, ...]

    column_types: dict[str, str]

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

        if not self.column_types:
            raise ValueError(
                "column_types không được để trống."
            )

        missing_type_columns = [
            column
            for column in self.select_columns
            if column not in self.column_types
        ]

        if missing_type_columns:
            raise ValueError(
                "Các column chưa có kiểu dữ liệu: "
                f"{missing_type_columns}"
            )

        extra_type_columns = [
            column
            for column in self.column_types
            if column not in self.select_columns
        ]

        if extra_type_columns:
            raise ValueError(
                "column_types chứa column không có "
                "trong select_columns: "
                f"{extra_type_columns}"
            )

        allowed_types = {
            "string",
            "int32",
            "int64",
            "decimal_10_2",
            "decimal_10_6",
            "timestamp_us",
        }

        invalid_types = {
            column: data_type
            for column, data_type
            in self.column_types.items()
            if data_type not in allowed_types
        }

        if invalid_types:
            raise ValueError(
                "Phát hiện kiểu dữ liệu không hỗ trợ: "
                f"{invalid_types}"
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

    column_types={
        "order_id": "string",
        "customer_id": "string",
        "warehouse_id": "string",
        "order_status": "string",

        "order_purchase_timestamp":
            "timestamp_us",

        "order_approved_at":
            "timestamp_us",

        "order_delivered_carrier_date":
            "timestamp_us",

        "order_delivered_customer_date":
            "timestamp_us",

        "order_estimated_delivery_date":
            "timestamp_us",

        "source_system": "string",

        "created_at":
            "timestamp_us",

        "updated_at":
            "timestamp_us",
    },
)

CUSTOMERS_CONFIG = IncrementalTableConfig(
    table_name="customers",

    primary_key_columns=(
        "customer_id",
    ),

    initial_primary_key_values=(
        "",
    ),

    watermark_column="updated_at",

    select_columns=(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "created_at",
        "updated_at",
    ),

    column_types={
        "customer_id": "string",
        "customer_unique_id": "string",
        "customer_zip_code_prefix": "string",
        "customer_city": "string",
        "customer_state": "string",
        "created_at": "timestamp_us",
        "updated_at": "timestamp_us",
    },
)

WAREHOUSES_CONFIG = IncrementalTableConfig(
    table_name="warehouses",
    primary_key_columns=(
        "warehouse_id",
    ),
    initial_primary_key_values=(
        "",
    ),
    watermark_column="updated_at",
    select_columns=(
        "warehouse_id",
        "warehouse_city",
        "warehouse_region",
        "created_at",
        "updated_at",
    ),

    column_types={
        "warehouse_id": "string",
        "warehouse_city": "string",
        "warehouse_region": "string",
        "created_at": "timestamp_us",
        "updated_at": "timestamp_us",
    },
)


PRODUCT_CATEGORY_TRANSLATION_CONFIG = (
    IncrementalTableConfig(
        table_name=(
            "product_category_name_translation"
        ),
        primary_key_columns=(
            "product_category_name",
        ),
        initial_primary_key_values=(
            "",
        ),
        watermark_column="updated_at",
        select_columns=(
            "product_category_name",
            "product_category_name_english",
            "created_at",
            "updated_at",
        ),

        column_types={
            "product_category_name": "string",
            "product_category_name_english":
                "string",
            "created_at": "timestamp_us",
            "updated_at": "timestamp_us",
        },
    )
)


SELLERS_CONFIG = IncrementalTableConfig(
    table_name="sellers",
    primary_key_columns=(
        "seller_id",
    ),
    initial_primary_key_values=(
        "",
    ),
    watermark_column="updated_at",
    select_columns=(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "created_at",
        "updated_at",
    ),

    column_types={
        "seller_id": "string",
        "seller_zip_code_prefix": "string",
        "seller_city": "string",
        "seller_state": "string",
        "created_at": "timestamp_us",
        "updated_at": "timestamp_us",
    },
)


PRODUCTS_CONFIG = IncrementalTableConfig(
    table_name="products",
    primary_key_columns=(
        "product_id",
    ),
    initial_primary_key_values=(
        "",
    ),
    watermark_column="updated_at",
    select_columns=(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "created_at",
        "updated_at",
    ),

    column_types={
        "product_id": "string",

        "product_category_name":
            "string",

        "product_name_lenght":
            "int32",

        "product_description_lenght":
            "int32",

        "product_photos_qty":
            "int32",

        "product_weight_g":
            "decimal_10_2",

        "product_length_cm":
            "decimal_10_2",

        "product_height_cm":
            "decimal_10_2",

        "product_width_cm":
            "decimal_10_2",

        "created_at":
            "timestamp_us",

        "updated_at":
            "timestamp_us",
    },
)


GEOLOCATION_CONFIG = IncrementalTableConfig(
    table_name="geolocation",
    primary_key_columns=(
        "geolocation_id",
    ),
    initial_primary_key_values=(
        0,
    ),
    watermark_column="updated_at",
    select_columns=(
        "geolocation_id",
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
        "created_at",
        "updated_at",
    ),

    column_types={
        "geolocation_id":
            "int64",

        "geolocation_zip_code_prefix":
            "string",

        "geolocation_lat":
            "decimal_10_6",

        "geolocation_lng":
            "decimal_10_6",

        "geolocation_city":
            "string",

        "geolocation_state":
            "string",

        "created_at":
            "timestamp_us",

        "updated_at":
            "timestamp_us",
    },
)

ORDER_ITEMS_CONFIG = IncrementalTableConfig(
    table_name="order_items",

    primary_key_columns=(
        "order_id",
        "order_item_id",
    ),

    initial_primary_key_values=(
        "",
        0,
    ),

    watermark_column="updated_at",

    select_columns=(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "warehouse_id",
        "quantity",
        "created_at",
        "updated_at",
    ),

    column_types={
        "order_id": "string",
        "order_item_id": "int32",
        "product_id": "string",
        "seller_id": "string",

        "shipping_limit_date":
            "timestamp_us",

        "price":
            "decimal_10_2",

        "freight_value":
            "decimal_10_2",

        "warehouse_id":
            "string",

        "quantity":
            "int32",

        "created_at":
            "timestamp_us",

        "updated_at":
            "timestamp_us",
    },
)


ORDER_PAYMENTS_CONFIG = IncrementalTableConfig(
    table_name="order_payments",

    primary_key_columns=(
        "order_id",
        "payment_sequential",
    ),

    initial_primary_key_values=(
        "",
        0,
    ),

    watermark_column="updated_at",

    select_columns=(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "created_at",
        "updated_at",
    ),

    column_types={
        "order_id": "string",

        "payment_sequential":
            "int32",

        "payment_type":
            "string",

        "payment_installments":
            "int32",

        "payment_value":
            "decimal_10_2",

        "created_at":
            "timestamp_us",

        "updated_at":
            "timestamp_us",
    },
)


ORDER_REVIEWS_CONFIG = IncrementalTableConfig(
    table_name="order_reviews",

    primary_key_columns=(
        "review_id",
        "order_id",
    ),

    initial_primary_key_values=(
        "",
        "",
    ),

    watermark_column="updated_at",

    select_columns=(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
        "created_at",
        "updated_at",
    ),

    column_types={
        "review_id": "string",
        "order_id": "string",

        "review_score":
            "int32",

        "review_comment_title":
            "string",

        "review_comment_message":
            "string",

        "review_creation_date":
            "timestamp_us",

        "review_answer_timestamp":
            "timestamp_us",

        "created_at":
            "timestamp_us",

        "updated_at":
            "timestamp_us",
    },
)

INVENTORY_CONFIG = IncrementalTableConfig(
    table_name="inventory",

    primary_key_columns=(
        "warehouse_id",
        "product_id",
    ),

    initial_primary_key_values=(
        "",
        "",
    ),

    watermark_column="updated_at",

    select_columns=(
        "warehouse_id",
        "product_id",
        "quantity_available",
        "created_at",
        "updated_at",
    ),

    column_types={
        "warehouse_id": "string",
        "product_id": "string",

        "quantity_available":
            "int32",

        "created_at":
            "timestamp_us",

        "updated_at":
            "timestamp_us",
    },
)

TABLE_CONFIGS = {
    ORDERS_CONFIG.table_name:
        ORDERS_CONFIG,

    CUSTOMERS_CONFIG.table_name:
        CUSTOMERS_CONFIG,

    WAREHOUSES_CONFIG.table_name:
        WAREHOUSES_CONFIG,

    PRODUCT_CATEGORY_TRANSLATION_CONFIG.table_name:
        PRODUCT_CATEGORY_TRANSLATION_CONFIG,

    SELLERS_CONFIG.table_name:
        SELLERS_CONFIG,

    PRODUCTS_CONFIG.table_name:
        PRODUCTS_CONFIG,

    GEOLOCATION_CONFIG.table_name:
        GEOLOCATION_CONFIG,

    ORDER_ITEMS_CONFIG.table_name:
        ORDER_ITEMS_CONFIG,

    ORDER_PAYMENTS_CONFIG.table_name:
        ORDER_PAYMENTS_CONFIG,

    ORDER_REVIEWS_CONFIG.table_name:
        ORDER_REVIEWS_CONFIG,
        
    INVENTORY_CONFIG.table_name:
        INVENTORY_CONFIG,
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

    print(
        "Testing incremental table configs..."
    )

    for table_name, config in (
        TABLE_CONFIGS.items()
    ):

        assert set(
            config.select_columns
        ) == set(
            config.column_types
        )

        print(
            f"[PASS] {table_name}"
        )

    print(
        "\nALL TABLE CONFIGS: PASS"
    )