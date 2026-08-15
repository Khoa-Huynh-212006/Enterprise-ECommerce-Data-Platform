from datetime import datetime
from azure.storage.filedatalake import FileSystemClient
from fastorder.ingestion.file_based.file_discovery import (
    DiscoveredFile,
)
from io import BytesIO
import pandas as pd
from zoneinfo import ZoneInfo

SOURCE_NAME = "yoochoose_clickstream"
INGESTION_METHOD = "file_incremental"
BRONZE_ROOT = "clickstream/yoochoose"

def write_file_to_bronze(
    *,
    landing_client: FileSystemClient,
    bronze_client: FileSystemClient,
    source_root: str,
    source_file: DiscoveredFile,
    ingestion_id: str,
    ingested_at: datetime,
) -> str:

    normalized_root = source_root.strip("/")

    source_path = (
        f"{normalized_root}/"
        f"{source_file.relative_path}"
    )

    source_file_client = landing_client.get_file_client(source_path) # Thêm object trỏ tới vị trí file

    download = source_file_client.download_file()
    source_bytes = download.readall() #trả về 1 python object kiểu Bytes nằm trong RAM

    downloaded_size = len(source_bytes)

    if downloaded_size != source_file.size:
        raise ValueError(
            "Downloaded source có kích thước không khớp: "
            f"path={source_file.relative_path}, "
            f"expected={source_file.size}, "
            f"actual={downloaded_size}"
        )

    source_df = pd.read_csv(
        BytesIO(source_bytes),
        header=None,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        on_bad_lines="error",
    )

    EXPECTED_SOURCE_COLUMNS = 4

    # dataframe.shape (rows, columns)
    if source_df.shape[1] != EXPECTED_SOURCE_COLUMNS:
        raise ValueError(
            "Số lượng cột không đúng với nguồn: "
            f"path={source_file.relative_path}, "
            f"expected={EXPECTED_SOURCE_COLUMNS}, "
            f"actual={source_df.shape[1]}"
        )

    source_df.columns = [
        "session_id",
        "event_timestamp",
        "item_id",
        "category",
    ]

    bronze_df = source_df.assign(
        _source_name=SOURCE_NAME,
        _source_file_path=source_file.relative_path,
        _source_file_etag=source_file.etag,
        _source_file_last_modified=source_file.last_modified,
        _source_file_size=source_file.size,
        _ingestion_id=ingestion_id,
        _ingested_at=ingested_at,
        _ingestion_method=INGESTION_METHOD,
    )

    if len(bronze_df) != len(source_df):
        raise ValueError(
            "Số dòng dữ liệu bị thay đổi sau khi thêm metadata"
        )

    parquet_buffer = BytesIO() # Tạo một vùng bộ nhớ RAM có giao diện giống một file.

    bronze_df.to_parquet(
        parquet_buffer,
        engine="pyarrow",
        index=False,
        compression="snappy",
    ) # lấy DataFrame này, serialize nó thành định dạng Parquet và ghi kết quả vào parquet_buffer (RAM)

    parquet_bytes = parquet_buffer.getvalue() # lấy toàn bộ nội dung hiện có trong BytesIO và trả về dạng:

    if not parquet_bytes:
        raise ValueError(
            "Parquet serialization tạo ra kết quả empty"
        )

    if not parquet_bytes.startswith(b"PAR1"):
        raise ValueError(
            "Serialized output không giống định dạng Parquet"
        )

    if not ingestion_id.strip():
        raise ValueError(
            "ingestion_id không được để trống"
        )

    if "/" in ingestion_id or "\\" in ingestion_id:
        raise ValueError(
            f"ingestion_id không hợp lệ: {ingestion_id}"
        )

    ingestion_date = (
        ingested_at
        .astimezone(ZoneInfo("Asia/Ho_Chi_Minh"))
        .date()
        .isoformat()
    )

    bronze_path = (
        f"{BRONZE_ROOT}/"
        f"ingestion_date={ingestion_date}/"
        f"ingestion_id={ingestion_id}/"
        "part-000.parquet"
    )

    bronze_file_client = bronze_client.get_file_client(bronze_path)

    bronze_file_client.upload_data(parquet_bytes,overwrite=True)

    return bronze_path