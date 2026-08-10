import io
import pandas as pd
from datetime import datetime
from azure.storage.filedatalake import FileSystemClient, DataLakeFileClient

from fastorder.storage.adls_client import get_bronze_file_system_client


def write_adls_bronze_batch(
    records: list[dict],
    table_name: str,
    extraction_id: str,
    ingested_at: datetime
) -> str:
    if not records:
        raise ValueError("Loi nghiep vu: Danh sach ban ghi 'records' dau vao bi rong!")
    if not table_name:
        raise ValueError("Loi nghiep vu: Tham so 'table_name' khong duoc de trong!")
    if not extraction_id:
        raise ValueError("Loi nghiep vu: Tham so 'extraction_id' khong duoc de trong!")
    if not isinstance(ingested_at, datetime):
        raise ValueError("Loi nghiep vu: Tham so 'ingested_at' phai la mot doi tuong datetime hop le!")

    df = pd.DataFrame(records)

    if "updated_at" not in df.columns:
        raise ValueError("Loi nghiep vu: records thieu cot 'updated_at' bat buoc cho incremental extraction.")

    df["_ingestion_id"] = extraction_id
    df["_ingested_at"] = pd.to_datetime(ingested_at)
    df["_source_table"] = table_name
    df["_source_updated_at"] = pd.to_datetime(df["updated_at"])
    df["_ingestion_method"] = "timestamp_incremental"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    
    buffer.seek(0)

    ingestion_date_str = ingested_at.strftime("%Y-%m-%d")
    remote_path = f"{table_name}/ingestion_date={ingestion_date_str}/extraction_id={extraction_id}/part-000.parquet"

    filesystem_client: FileSystemClient = get_bronze_file_system_client()
    file_client: DataLakeFileClient = filesystem_client.get_file_client(remote_path)

    print(f"Dang tien hanh tai luong du lieu Parquet len ADLS tai duong dan: {remote_path}")
    file_client.upload_data(data=buffer, overwrite=True)

    properties = file_client.get_file_properties()
    if properties.size == 0:
        raise RuntimeError(f"Loi he thong: File Parquet da duoc ghi tai '{remote_path}' nhung dung luong bang 0 bytes!")

    return remote_path

if __name__ == "__main__":
    
    print("BAT DAU TEST: ADLS Bronze Writer & Idempotency")
    
    
    test_records = [
        {"order_id": "O001", "status": "NEW", "updated_at": "2026-08-10T12:00:00.000000"},
        {"order_id": "O002", "status": "SHIPPED", "updated_at": "2026-08-10T12:05:00.000000"},
        {"order_id": "O003", "status": "COMPLETED", "updated_at": "2026-08-10T12:10:00.000000"}
    ]
    
    test_ingested_at = datetime.now()
    test_extraction_id = "test_adls_writer_001"
    test_table_name = "orders"
    
    try:
        print("1. Thuc thi write_adls_bronze_batch (Lan 1)...")
        path_1 = write_adls_bronze_batch(
            records=test_records,
            table_name=test_table_name,
            extraction_id=test_extraction_id,
            ingested_at=test_ingested_at
        )
        
        expected_prefix = (
            f"orders/ingestion_date={test_ingested_at.strftime('%Y-%m-%d')}/"
            f"extraction_id={test_extraction_id}/"
        )
        assert path_1.startswith(expected_prefix), "Storage contract violation: prefix"
        assert path_1.endswith("/part-000.parquet"), "Storage contract violation: suffix"
        print(f"   [PASS] Upload lan 1 thanh cong, path_1: {path_1}")

        print("\n2. Thuc thi write_adls_bronze_batch Y HET (Lan 2 - Retry)...")
        path_2 = write_adls_bronze_batch(
            records=test_records,
            table_name=test_table_name,
            extraction_id=test_extraction_id,
            ingested_at=test_ingested_at
        )
        print(f"   [PASS] Upload lan 2 thanh cong, path_2: {path_2}")

        print("\n3. Kiem tra tinh nhat quan cua duong dan...")
        assert path_1 == path_2, "Idempotency violation: Duong dan thay doi sau khi retry."
        print("   [PASS] Duong dan khong doi: path_1 == path_2.")

        print("\n4. Kiem tra so luong file thuc te tren ADLS directory...")
        fs_client = get_bronze_file_system_client()
        
        dir_path = path_1.rsplit('/', 1)[0]
        paths = list(
            fs_client.get_paths(
                path=dir_path,
                recursive=False
            )
        )
        
        files_in_dir = [p.name for p in paths if not p.is_directory]
        
        print(f"   Files found: {files_in_dir}")
        assert len(files_in_dir) == 1, f"Idempotency violation: Co nhieu hon 1 file. Tim thay: {len(files_in_dir)}"
        assert set(files_in_dir) == {path_1}, "Ten file khong dung chuan part-000.parquet"
        print("   [PASS] Chi co dung 1 file part-000.parquet ton tai (Ghi de thanh cong, khong rac).")

        print("\n5. Download file sau Lan 2 de kiem tra du lieu...")
        file_client = fs_client.get_file_client(path_2)
        downloaded_bytes = file_client.download_file().readall()
        
        df_read = pd.read_parquet(io.BytesIO(downloaded_bytes))
        
        assert len(df_read) == 3, f"So luong row sai, mong doi 3 nhung nhan {len(df_read)}"
        assert len(df_read["order_id"].unique()) == 3, "Du lieu bi duplicate sau khi overwrite!"
        
        expected_meta_cols = [
            "_ingestion_id", "_ingested_at", "_source_table", 
            "_source_updated_at", "_ingestion_method"
        ]
        for col in expected_meta_cols:
            assert col in df_read.columns, f"Thieu metadata column: {col}"
            
        assert not df_read["_source_updated_at"].isnull().all(), "_source_updated_at chua toan NaT"
        assert df_read["_ingestion_id"].eq(test_extraction_id).all(), "Data contract violation: _ingestion_id khong khop"
        assert df_read["_ingestion_method"].eq("timestamp_incremental").all(), "Data contract violation: _ingestion_method khong khop"
        
        print("   [PASS] Read Parquet sau Retry thanh cong. Row count = 3, khong duplicate. Metadata hoan hao.")
        print("ADLS BRONZE WRITER & IDEMPOTENCY TEST COMPLETE: PERFECT PASS")
        
    except Exception as e:
        print(f"\n[FAIL] Test bi loi: {type(e).__name__} - {e}")
        
        if "PathNotFound" in str(e) or "BlobNotFound" in str(e):
            print(
                "\n[NOTE] ADLS bao loi path. "
                "Can kiem tra directory hierarchy truoc khi thay doi writer."
            )