from typing import Dict, Any, Optional
from pathlib import Path
import json 
import os

PENDING_CONTEXT_VERSION = 1 

def build_pending_batch_context(
    table_name: str,
    run_id:str, 
    run_upper_watermark: Dict[str, str],
    lower_watermark: Dict[str, str],
    batch_upper_watermark: Dict[str, str],
    extraction_id: str,
    ingested_at: str,
    batch_size: int
)-> Dict[str, Any]:
    """ 
    Tạo dictionary cấu trúc của Pending Batch Context theo chuẩn schema.
    """
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
    """
    Kiểm định cấu trúc và nội dung của Pending Context.
    """
    if "version" not in context or context["version"] != PENDING_CONTEXT_VERSION:
        raise ValueError(f"Pending context version không hợp lệ. Kỳ vọng: {PENDING_CONTEXT_VERSION}")

    if context.get("table_name") != expected_table_name:
        raise ValueError(f"Table name sai lệch. Kỳ vọng: '{expected_table_name}', Nhận: '{context.get('table_name')}'")

    required_keys = [
        "run_id", "run_upper_watermark", "lower_watermark", 
        "batch_upper_watermark", "extraction_id", "ingested_at", "batch_size"
    ]
    for key in required_keys:
        if key not in context:
            raise ValueError(f"Pending context thiếu trường bắt buộc: '{key}'")

    # Kiểm tra sâu các object Watermark
    watermark_keys = ["run_upper_watermark", "lower_watermark", "batch_upper_watermark"]
    for wm_key in watermark_keys:
        wm_obj = context[wm_key]
        if not isinstance(wm_obj, dict) or "updated_at" not in wm_obj or "order_id" not in wm_obj:
            raise ValueError(f"Watermark '{wm_key}' sai cấu trúc. Cần có 'updated_at' và 'order_id'.")


def load_pending_batch_context(
    file_path: Path, 
    expected_table_name: str
    ) -> Optional[Dict[str, Any]]:
    """ 
    Đọc Pending Context từ đĩa.
    - Trả về None nếu file chưa tồn tại (New Run / No pending).
    - Quăng lỗi nếu JSON hỏng hoặc không đúng chuẩn (Tránh tự đoán).
    """
    if not file_path.exists():
        return None

    try: 
        with open(file_path, "r", encoding="utf-8") as f:
            context = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"File pending context bị hỏng cấu trúc JSON: {e}") from e

    validate_pending_batch_context(context, expected_table_name)

    return context 

def save_pending_batch_context_atomic(file_path: Path, context: Dict[str, Any]) -> None:
    """
    Lưu Pending Context an toàn tuyệt đối thông qua file tạm và hệ điều hành.
    """

    validate_pending_batch_context(context, context.get("table_name", ""))

    tmp_path = file_path.with_suffix(".tmp")

    file_path.parent.mkdir(parents= True, exist_ok= True)

    try: 
        with open(tmp_path, "w", encoding= "utf-8") as f:
            json.dump(context, f, indent= 2)
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
    """
    Xóa file Pending Context sau khi Checkpoint đã được ghi nhận thành công.
    Không văng lỗi nếu file đã biến mất.
    """
    try:
        file_path.unlink(missing_ok=True)
    except Exception as e:
        raise RuntimeError(f"Lỗi khi xóa pending batch context: {e}") from e


if __name__ == "__main__":
    print("Bắt đầu Smoke Test: Pending Batch Manager\n" + "-"*50)
    
    test_path = Path("test_pending_context.json")
    if test_path.exists(): test_path.unlink()
    if test_path.with_suffix(".tmp").exists(): test_path.with_suffix(".tmp").unlink()
    
    try:
        # 1. File chưa tồn tại -> load trả None
        assert load_pending_batch_context(test_path, "orders") is None, "Lỗi: File không tồn tại phải trả None."
        print("Test 1: Load file chưa tồn tại trả về None.")

        # Dữ liệu giả lập chuẩn
        valid_context = build_pending_batch_context(
            table_name="orders",
            run_id="run_2026",
            run_upper_watermark={"updated_at": "2026-08-06T15:00:00.000", "order_id": "999"},
            lower_watermark={"updated_at": "2026-08-01T00:00:00.000", "order_id": "000"},
            batch_upper_watermark={"updated_at": "2026-08-02T00:00:00.000", "order_id": "111"},
            extraction_id="run_2026_batch_001",
            ingested_at="2026-08-06T15:01:00.000",
            batch_size=1000
        )

        # 2. Save context -> load lại giống hoàn toàn
        save_pending_batch_context_atomic(test_path, valid_context)
        loaded_context = load_pending_batch_context(test_path, "orders")
        assert loaded_context == valid_context, "Lỗi: Dữ liệu load lên bị sai lệch."
        print("Test 2: Save atomic và Load dữ liệu nguyên vẹn.")

        # 6. Atomic save không còn .tmp
        assert not test_path.with_suffix(".tmp").exists(), "Lỗi: Tồn dư file .tmp rác!"
        print("Test 6: Atomic save dọn dẹp file .tmp sạch sẽ.")

        # 3. JSON lỗi -> raise
        with open(test_path, "w", encoding="utf-8") as f:
            f.write("{ json hỏng rồi nha")
        try:
            load_pending_batch_context(test_path, "orders")
            assert False, "Lỗi: Không bắt được lỗi JSON hỏng!"
        except ValueError as e:
            assert "JSON" in str(e)
            print("Test 3: Chặn JSON hỏng thành công.")

        # 4. Sai table_name -> raise
        save_pending_batch_context_atomic(test_path, valid_context) # Ghi lại đồ xịn
        try:
            load_pending_batch_context(test_path, "products")
            assert False, "Lỗi: Không bắt được lỗi sai table_name!"
        except ValueError as e:
            assert "Table name sai lệch" in str(e)
            print("Test 4: Chặn sai table_name thành công.")

        # 5. Thiếu watermark -> raise
        invalid_context = valid_context.copy()
        del invalid_context["batch_upper_watermark"]
        try:
            save_pending_batch_context_atomic(test_path, invalid_context)
            assert False, "Lỗi: Không bắt được lỗi thiếu trường bắt buộc!"
        except ValueError as e:
            assert "thiếu trường bắt buộc" in str(e)
            print("Test 5: Chặn thiếu trường dữ liệu thành công.")

        # 7. Delete -> file biến mất
        save_pending_batch_context_atomic(test_path, valid_context) # Đảm bảo file đang có
        delete_pending_batch_context(test_path)
        assert not test_path.exists(), "Lỗi: Delete xong file vẫn còn!"
        print("Test 7: Xóa file thành công.")

        # 8. Delete lần hai -> không lỗi
        try:
            delete_pending_batch_context(test_path)
            print("Test 8: Xóa file lần 2 (không tồn tại) không văng lỗi.")
        except Exception:
            assert False, "Lỗi: Xóa file lần 2 bị văng lỗi!"

    finally:
        # Dọn dẹp
        if test_path.exists(): test_path.unlink()
        if test_path.with_suffix(".tmp").exists(): test_path.with_suffix(".tmp").unlink()
        print("\nHoàn tất dọn dẹp file test.")

    