import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

PENDING_CONTEXT_VERSION = 1
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

def _parse_timestamp(value: str) -> datetime:
    """Validate và parse timestamp chuẩn canonical ISO 8601 Naive (6 chữ số thập phân)."""
    if not isinstance(value, str):
        raise ValueError("updated_at phải là chuỗi.")

    try:
        parsed = datetime.strptime(value, TIMESTAMP_FORMAT)
    except ValueError as error:
        raise ValueError(f"Timestamp không hợp lệ: '{value}'.") from error

    # Đảm bảo đúng 6 chữ số thập phân, không bị cắt xén
    if parsed.strftime(TIMESTAMP_FORMAT) != value:
        raise ValueError(f"Timestamp không đúng canonical format: '{value}'.")

    return parsed

def _watermark_key(watermark: Dict[str, str]):
    """Tạo tuple (datetime, order_id) để so sánh thứ tự Composite Watermark."""
    return (
        _parse_timestamp(watermark["updated_at"]),
        watermark["order_id"]
    )

def build_pending_batch_context(
    table_name: str,
    run_id: str,
    run_upper_watermark: Dict[str, str],
    lower_watermark: Dict[str, str],
    batch_upper_watermark: Dict[str, str],
    extraction_id: str,
    ingested_at: str,
    batch_size: int
) -> Dict[str, Any]:
    """Tạo dictionary cấu trúc của Pending Batch Context."""
    return {
        "version": PENDING_CONTEXT_VERSION,
        "table_name": table_name,
        "run_id": run_id,
        "run_upper_watermark": run_upper_watermark,
        "lower_watermark": lower_watermark,
        "batch_upper_watermark": batch_upper_watermark,
        "extraction_id": extraction_id,
        "ingested_at": ingested_at,
        "batch_size": batch_size
    }

def validate_pending_batch_context(context: Dict[str, Any], expected_table_name: str) -> None:
    """Kiểm định chặt chẽ cấu trúc, kiểu dữ liệu và định luật Watermark của Pending Context."""
    
    # 1. Validation Cơ bản
    if not isinstance(context, dict):
        raise ValueError("Pending context phải là dictionary.")
    if not isinstance(expected_table_name, str) or not expected_table_name.strip():
        raise ValueError("expected_table_name phải là chuỗi không rỗng.")

    if context.get("version") != PENDING_CONTEXT_VERSION:
        raise ValueError(f"Pending context version không hợp lệ. Kỳ vọng: {PENDING_CONTEXT_VERSION}")

    if context.get("table_name") != expected_table_name:
        raise ValueError(f"Table name sai lệch. Kỳ vọng: '{expected_table_name}', Nhận: '{context.get('table_name')}'")

    # 2. Kiểm tra các trường chuỗi bắt buộc
    for key in ("table_name", "run_id", "extraction_id", "ingested_at"):
        value = context.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Trường '{key}' phải là chuỗi không rỗng.")

    # 3. Kiểm tra Integer (Chặn boolean hack của Python)
    batch_size = context.get("batch_size")
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
        raise ValueError("batch_size phải là số nguyên > 0.")

    # 4. Kiểm tra cấu trúc các khối Watermark
    watermark_keys = ["run_upper_watermark", "lower_watermark", "batch_upper_watermark"]
    for wm_key in watermark_keys:
        wm_obj = context.get(wm_key)
        if not isinstance(wm_obj, dict):
            raise ValueError(f"Watermark '{wm_key}' bị thiếu hoặc không phải dictionary.")
        if "updated_at" not in wm_obj or "order_id" not in wm_obj:
            raise ValueError(f"Watermark '{wm_key}' sai cấu trúc. Cần có 'updated_at' và 'order_id'.")
        if not isinstance(wm_obj["order_id"], str):
            raise ValueError(f"order_id trong '{wm_key}' phải là chuỗi.")

    # 5. Kiểm tra Định luật thứ tự Watermark (Invariant: lower < batch_upper <= run_upper)
    lower_key = _watermark_key(context["lower_watermark"])
    batch_upper_key = _watermark_key(context["batch_upper_watermark"])
    run_upper_key = _watermark_key(context["run_upper_watermark"])

    if not (lower_key < batch_upper_key <= run_upper_key):
        raise ValueError(
            "Watermark phải thỏa mãn định luật: lower_watermark < batch_upper_watermark <= run_upper_watermark. "
            f"Thực tế: lower={lower_key}, batch_upper={batch_upper_key}, run_upper={run_upper_key}"
        )

def load_pending_batch_context(file_path: Path, expected_table_name: str) -> Optional[Dict[str, Any]]:
    """Đọc và Validate Pending Context từ đĩa."""
    if not file_path.exists():
        return None
        
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            context = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"File pending context bị hỏng cấu trúc JSON: {e}") from e

    validate_pending_batch_context(context, expected_table_name)
    return context

def save_pending_batch_context_atomic(file_path: Path, context: Dict[str, Any], expected_table_name: str) -> None:
    """Lưu Pending Context. Truyền expected_table_name từ Caller để chống giả mạo."""
    validate_pending_batch_context(context, expected_table_name)
    
    # Sử dụng f-string nối tên gốc và .tmp để tránh nhầm lẫn đuôi file
    tmp_path = Path(f"{file_path}.tmp")
    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(context, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
            
        os.replace(tmp_path, file_path)
    except Exception as e:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise RuntimeError(f"Lỗi khi lưu pending batch context: {e}") from e

def delete_pending_batch_context(file_path: Path) -> None:
    """Xóa file Pending Context (Idempotent)."""
    try:
        file_path.unlink(missing_ok=True)
    except Exception as e:
        raise RuntimeError(f"Lỗi khi xóa pending batch context: {e}") from e


if __name__ == "__main__":
    print("Bắt đầu Smoke Test: Pending Batch Manager\n" + "-"*50)
    
    test_path = Path("test_pending_context.json")
    tmp_path = Path(f"{test_path}.tmp")
    
    # Cleanup ban đầu
    if test_path.exists(): test_path.unlink()
    if tmp_path.exists(): tmp_path.unlink()
    
    try:
        # 1. File chưa tồn tại
        assert load_pending_batch_context(test_path, "orders") is None
        print("Test 1: Load file chưa tồn tại trả về None.")

        # Dữ liệu chuẩn (6 chữ số microsecond)
        valid_context = build_pending_batch_context(
            table_name="orders",
            run_id="run_2026",
            run_upper_watermark={"updated_at": "2026-08-06T15:00:00.000000", "order_id": "999"},
            lower_watermark={"updated_at": "2026-08-01T00:00:00.000000", "order_id": "000"},
            batch_upper_watermark={"updated_at": "2026-08-02T00:00:00.000000", "order_id": "111"},
            extraction_id="run_2026_batch_001",
            ingested_at="2026-08-06T15:01:00.000000",
            batch_size=1000
        )

        # 2. Save atomic
        save_pending_batch_context_atomic(test_path, valid_context, "orders")
        loaded_context = load_pending_batch_context(test_path, "orders")
        assert loaded_context == valid_context
        print("Test 2: Save atomic và Load dữ liệu nguyên vẹn.")

        # 3. Không có rác .tmp
        assert not tmp_path.exists()
        print("Test 3: Tên file tạm chuẩn và dọn dẹp sạch sẽ.")

        # 4. JSON hỏng
        with open(test_path, "w", encoding="utf-8") as f:
            f.write("{ json hỏng }")
        try:
            load_pending_batch_context(test_path, "orders")
            assert False
        except ValueError:
            pass
        print("Test 4: Chặn JSON hỏng thành công.")

        # 5. Invalid save không phá file hợp lệ cũ
        save_pending_batch_context_atomic(test_path, valid_context, "orders")
        before_save = load_pending_batch_context(test_path, "orders")
        
        invalid_context = valid_context.copy()
        invalid_context["table_name"] = "products"
        try:
            save_pending_batch_context_atomic(test_path, invalid_context, "orders")
            assert False
        except ValueError:
            pass
            
        after_save = load_pending_batch_context(test_path, "orders")
        assert after_save == before_save
        print("Test 5: Validation bảo vệ file hợp lệ không bị ghi đè bởi Data xấu.")

        # 6. Chặn Batch Size sai
        for bad_size in [0, -1, True, "1000"]:
            bad_ctx = valid_context.copy()
            bad_ctx["batch_size"] = bad_size
            try:
                save_pending_batch_context_atomic(test_path, bad_ctx, "orders")
                assert False, f"Không chặn được batch_size={bad_size}"
            except ValueError:
                pass
        print("Test 6: Chặn mọi định dạng batch_size dị biệt.")

        # 7. Chặn Watermark sai thứ tự
        # TH1: batch_upper < lower
        bad_wm_1 = valid_context.copy()
        bad_wm_1["batch_upper_watermark"] = {"updated_at": "2026-07-01T00:00:00.000000", "order_id": "000"}
        
        # TH2: batch_upper == lower
        bad_wm_2 = valid_context.copy()
        bad_wm_2["batch_upper_watermark"] = valid_context["lower_watermark"]
        
        # TH3: run_upper < batch_upper
        bad_wm_3 = valid_context.copy()
        bad_wm_3["run_upper_watermark"] = {"updated_at": "2026-08-01T12:00:00.000000", "order_id": "000"}

        for bad_ctx in [bad_wm_1, bad_wm_2, bad_wm_3]:
            try:
                save_pending_batch_context_atomic(test_path, bad_ctx, "orders")
                assert False, "Lọt lưới Watermark sai thứ tự!"
            except ValueError as e:
                assert "thỏa mãn định luật" in str(e)
        print("Test 7: Định luật Watermark Invariant hoạt động hoàn hảo.")

        # 8. Delete Idempotent
        delete_pending_batch_context(test_path)
        assert not test_path.exists()
        delete_pending_batch_context(test_path) # Gọi lần 2
        print("Test 8: Xóa file thành công và Idempotent.")

    finally:
        if test_path.exists(): test_path.unlink()
        if tmp_path.exists(): tmp_path.unlink()
        print("\nHoàn tất dọn dẹp file test.")