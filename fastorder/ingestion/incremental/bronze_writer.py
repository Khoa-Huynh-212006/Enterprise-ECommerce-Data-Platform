import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict

def write_bronze_batch(
    records: List[Dict],
    bronze_root: Path,
    table_name: str,
    extraction_id: str,
    ingested_at: datetime
) -> Path:
    """
    Ghi một batch dữ liệu thành file Parquet an toàn tại tầng Bronze.
    Không query DB, không đụng chạm checkpoint. 
    Chỉ tập trung bảo đảm metadata chuẩn xác và file ghi dạng Atomic.
    """
    
    # 1. Validate các input quan trọng
    if not records:
        raise ValueError("Không thể ghi batch rỗng. Danh sách records đang trống.")
    if not table_name:
        raise ValueError("Tên bảng (table_name) không được để trống.")
    if not extraction_id:
        raise ValueError("ID của lượt trích xuất (extraction_id) không được để trống.")
    if not isinstance(ingested_at, datetime):
        raise ValueError("ingested_at bắt buộc phải là một đối tượng datetime.")

    # 2. Chuyển đổi sang Pandas DataFrame
    df = pd.DataFrame(records)

    # 3. Đảm bảo cột gốc tồn tại và KHÔNG chứa NULL
    if "updated_at" not in df.columns:
        raise ValueError("Trong records bị thiếu cột 'updated_at' (yêu cầu để ánh xạ metadata).")
    
    if df["updated_at"].isna().any():
        raise ValueError(
            "Cột 'updated_at' chứa giá trị NULL, "
            "không thể tạo metadata _source_updated_at."
        )

    # 4. Bổ sung Ingestion Metadata
    df["_ingestion_id"] = extraction_id
    df["_ingested_at"] = ingested_at
    df["_source_table"] = table_name
    df["_source_updated_at"] = df["updated_at"]
    df["_ingestion_method"] = "timestamp_incremental"

    # 5. Thiết lập Output Path Contract
    ingestion_date = ingested_at.strftime("%Y-%m-%d")
    output_dir = bronze_root / table_name / f"ingestion_date={ingestion_date}" / f"extraction_id={extraction_id}"
    
    output_dir.mkdir(parents=True, exist_ok=True)

    # Sửa lỗi with_suffix bằng cách nối chuỗi tường minh
    final_path = output_dir / "part-000.parquet"
    tmp_path = output_dir / f"{final_path.name}.tmp"

    # 6. Ghi dữ liệu vào file tạm
    try:
        df.to_parquet(tmp_path, index=False, engine="pyarrow")
    except Exception as e:
        if tmp_path.exists():
            tmp_path.unlink()
        raise ValueError(f"Lỗi hệ thống khi serialize Parquet: {e}") from e

    # 7. Kiểm tra an toàn trước khi Replace
    if not tmp_path.exists() or tmp_path.stat().st_size == 0:
        if tmp_path.exists():
            tmp_path.unlink()
        raise ValueError("Ghi Parquet thất bại: file tạm rỗng (size = 0) hoặc không tồn tại.")

    # 8. Atomic File Replacement (Sẽ ghi đè file cũ nếu Retry)
    try:
        os.replace(tmp_path, final_path)
    except OSError as e:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise ValueError(f"Lỗi IO khi thực hiện Atomic Replace file Parquet: {e}") from e

    return final_path


if __name__ == "__main__":
    import shutil
    from fastorder.db.connection import get_engine
    from fastorder.ingestion.incremental.orders_extractor import get_upper_watermark, extract_orders_batch

    print("Bắt đầu Smoke Test: Bronze Writer\n")
    
    test_bronze_root = Path("test_bronze_data")
    engine = get_engine()

    try:
        with engine.connect() as conn:
            upper_wm = get_upper_watermark(conn)
            
            if not upper_wm:
                raise RuntimeError("Smoke test thất bại: bảng orders không có dữ liệu.")
            
            initial_lower_wm = {
                "updated_at": "1970-01-01T00:00:00.000000",
                "order_id": ""
            }
            
            print("1. Đang trích xuất 5 records từ PostgreSQL...")
            records, _ = extract_orders_batch(
                conn=conn, 
                lower_watermark=initial_lower_wm, 
                upper_watermark=upper_wm, 
                batch_size=5
            )

            if not records:
                raise RuntimeError("Smoke test thất bại: extractor không trả về record nào.")

            print("2. Đang ghi file Parquet xuống Bronze Layer...")
            extraction_id = "test_extraction_001"
            ingested_at = datetime.now()
            
            output_path = write_bronze_batch(
                records=records,
                bronze_root=test_bronze_root,
                table_name="orders",
                extraction_id=extraction_id,
                ingested_at=ingested_at
            )
            
            print(f"   -> Đã ghi thành công tại: {output_path}")

            # 3. Assertions Test Chất lượng
            print("\n3. Đang kiểm định file Parquet (Lần 1)...")
            written_df = pd.read_parquet(output_path)
            
            assert len(written_df) == len(records), "Row count mismatch!"
            assert written_df["_ingestion_id"].eq(extraction_id).all(), "_ingestion_id sai!"
            assert written_df["_source_table"].eq("orders").all(), "_source_table sai!"
            assert written_df["_ingestion_method"].eq("timestamp_incremental").all(), "_ingestion_method sai!"
            pd.testing.assert_series_equal(
                written_df["_source_updated_at"],
                written_df["updated_at"],
                check_names=False
            )
            
            assert output_path.name == "part-000.parquet", "Tên file đầu ra sai!"
            assert f"ingestion_date={ingested_at:%Y-%m-%d}" in output_path.parts, "Sai định dạng thư mục Partition ngày!"
            assert f"extraction_id={extraction_id}" in output_path.parts, "Thiếu Extraction ID trong Path!"
            
            input_ids = [r["order_id"] for r in records]
            output_ids = written_df["order_id"].tolist()
            assert input_ids == output_ids, "Order ID không khớp 100%!"
            
            expected_tmp_path = output_path.parent / f"{output_path.name}.tmp"
            assert not expected_tmp_path.exists(), f"File tạm vẫn còn tồn tại: {expected_tmp_path}"
            
            print("   Vượt qua toàn bộ bài test cơ bản.")

            print("\n4. Đang kiểm tra cơ chế Retry (Idempotent Overwrite)...")
            
            # Cố tình gọi lại hàm ghi với y hệt tham số cũ
            retry_output_path = write_bronze_batch(
                records=records,
                bronze_root=test_bronze_root,
                table_name="orders",
                extraction_id=extraction_id,
                ingested_at=ingested_at
            )

            # Assert: Path phải y hệt
            assert retry_output_path == output_path, "Retry cùng extraction_id phải trả về cùng output path."
            print("   Assertion 6: Retry path không thay đổi.")

            # Assert: Không có file nhân bản nào được sinh ra trong toàn bộ thư mục root
            parquet_files = list(test_bronze_root.rglob("*.parquet"))
            assert len(parquet_files) == 1, f"Retry sinh ra nhiều hơn một file Parquet: {parquet_files}"
            assert parquet_files[0] == output_path, "File Parquet duy nhất không khớp path gốc."
            print("   Assertion 7: Chỉ tồn tại duy nhất 1 file Parquet (Ghi đè thành công).")

            # Assert: Nội dung và Metadata bên trong file retry bảo toàn nguyên vẹn
            retry_df = pd.read_parquet(retry_output_path)
            assert len(retry_df) == len(records), "Row count bị lỗi sau retry."
            assert retry_df["_ingestion_id"].eq(extraction_id).all(), "_ingestion_id sai sau retry."
            assert retry_df["_ingested_at"].eq(pd.Timestamp(ingested_at)).all(), "_ingested_at bị lệch sau retry."
            print("   Assertion 8: Nội dung và Ingestion Metadata không bị suy biến.")

            # Assert: Kiểm tra vét cạn toàn bộ cây thư mục xem có rác .tmp không
            tmp_files = list(test_bronze_root.rglob("*.tmp"))
            assert not tmp_files, f"Còn tồn tại file tạm sau retry ở đâu đó: {tmp_files}"
            print("   Assertion 9: Không còn bất kỳ file .tmp rác nào trong toàn hệ thống.")

    finally:
        if test_bronze_root.exists():
            shutil.rmtree(test_bronze_root)
            print("\nĐã dọn dẹp sạch sẽ thư mục test_bronze_data.")