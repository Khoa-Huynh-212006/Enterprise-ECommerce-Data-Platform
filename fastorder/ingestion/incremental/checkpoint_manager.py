import os
import json
from datetime import datetime
from pathlib import Path

def build_initial_checkpoint(table_name: str) -> dict:
    """
    Tạo checkpoint mặc định cho lần trích xuất đầu tiên
    """
    return {
        "version": 1,
        "table_name": table_name, 
        "watermark": {
            "updated_at": "1970-01-01T00:00:00.000000",
            "order_id": ""
        }
    }


def validate_checkpoint(checkpoint: dict, expected_table_name: str) -> None:
    """
    Kiểm tra checkpoint có hợp lệ hay không
    """
    if not isinstance(checkpoint, dict):
        raise ValueError("Checkpoint phải là một dictionary.")

    version = checkpoint.get("version")
    if version != 1:
        raise ValueError(
            f"Phiên bản checkpoint không được hỗ trợ: {version}. "
            "Kỳ vọng: 1."
        )

    if checkpoint.get("table_name") != expected_table_name:
        raise ValueError(
            f"Checkpoint không hợp lệ cho bảng "
            f"'{expected_table_name}'."
        )

    watermark = checkpoint.get("watermark")
    if not isinstance(watermark, dict):
        raise ValueError("Watermark phải là một dictionary.")

    updated_at = watermark.get("updated_at")
    if not isinstance(updated_at, str):
        raise ValueError("Watermark 'updated_at' phải là một chuỗi.")

    try:
        datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError as e:
        raise ValueError(
            "Watermark 'updated_at' phải có định dạng "
            "YYYY-MM-DDTHH:MM:SS.ffffff."
        ) from e

    order_id = watermark.get("order_id")
    if not isinstance(order_id, str):
        raise ValueError("Watermark 'order_id' phải là một chuỗi.")

    if order_id != "" and len(order_id) != 32:
        raise ValueError(
            "Watermark 'order_id' phải rỗng "
            "hoặc có đúng 32 ký tự."
        )


def load_checkpoint(checkpoint_path: Path, expected_table_name: str) -> dict:
    """
    Load checkpoint từ file. 
    Chỉ bắt các exception liên quan đến IO file hệ thống để bảo toàn traceback.
    """
    if not checkpoint_path.exists():
        return build_initial_checkpoint(expected_table_name)

    try:
        with checkpoint_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Checkpoint file '{checkpoint_path}' không hợp lệ: {e}") from e
    except (OSError, UnicodeError) as e:
        raise ValueError(f"Không thể đọc file checkpoint '{checkpoint_path}': {e}") from e

    validate_checkpoint(data, expected_table_name)
    return data


def save_checkpoint_atomic(
    checkpoint_path: Path, 
    checkpoint: dict, 
    expected_table_name: str
) -> None:
    """
    Lưu checkpoint theo cơ chế Atomic Write.
    - Validate cấu trúc dựa trên expected_table_name.
    - Ghi vào file .tmp và ép OS flush xuống đĩa cứng (fsync).
    - Đổi tên đè sang file chính thức bằng lệnh Atomic của OS.
    - Đảm bảo dọn dẹp file .tmp nếu có bất kỳ lỗi nào xảy ra.
    """
    validate_checkpoint(checkpoint, expected_table_name)

    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = checkpoint_path.with_suffix(".tmp")

    # 1. Ghi file tạm và ép dữ liệu vật lý xuống đĩa cứng
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=4)
            f.flush()
            os.fsync(f.fileno())
    except (OSError, UnicodeError) as e:
        if tmp_path.exists():
            tmp_path.unlink()
        raise ValueError(f"Lỗi khi ghi file checkpoint tạm: {e}") from e

    # 2. Ghi đè Atomic (Thay thế file cũ nếu có)
    try:
        os.replace(tmp_path, checkpoint_path)
    except OSError as e:
        # Cleanup file tạm nếu replace thất bại
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise ValueError(f"Lỗi khi thực hiện Atomic Replace: {e}") from e


if __name__ == "__main__":
    test_file = Path("test_orders_checkpoint.json")
    write_test_file = Path("test_atomic_checkpoint.json")

    print("Bắt đầu Smoke Test: Checkpoint Manager\n" + "-"*50)

    try:
        # 1. Trường hợp 1: File chưa tồn tại
        if test_file.exists():
            test_file.unlink()
        
        cp1 = load_checkpoint(test_file, "orders")
        print(f" Trường hợp 1 (Không có file)   : Pass. -> Khởi tạo: {cp1['watermark']}")

        # 2. Trường hợp 2: File hợp lệ
        valid_data = {
            "version": 1,
            "table_name": "orders",
            "watermark": {
                "updated_at": "2026-08-04T15:30:00.123456",
                "order_id": "a18fc8cb1234567890abcdef12345678"
            }
        }
        with test_file.open('w', encoding='utf-8') as f:
            json.dump(valid_data, f)
            
        cp2 = load_checkpoint(test_file, "orders")
        print(f" Trường hợp 2 (File hợp lệ)     : Pass. -> Dữ liệu: {cp2['watermark']}")

        # 3. Trường hợp 3: JSON hỏng cấu trúc
        with test_file.open('w', encoding='utf-8') as f:
            f.write('{"version": 1, "table_name": "orders", "water') 
            
        try:
            load_checkpoint(test_file, "orders")
            print("Trường hợp 3: THẤT BẠI. Lẽ ra phải văng Exception!")
        except ValueError as e:
            print(f" Trường hợp 3 (File hỏng)       : Pass. -> Đã chặn: {e}")

        # 4. Trường hợp 4: Sai phiên bản version
        invalid_version = valid_data.copy()
        invalid_version["version"] = 2
        with test_file.open('w', encoding='utf-8') as f:
            json.dump(invalid_version, f)
            
        try:
            load_checkpoint(test_file, "orders")
            print("Trường hợp 4: THẤT BẠI. Lẽ ra phải văng Exception!")
        except ValueError as e:
            print(f"Trường hợp 4 (Sai version)     : Pass. -> Đã chặn: {e}")

        # 5. Trường hợp 5: Sai tên bảng (table_name)
        wrong_table = valid_data.copy()
        wrong_table["table_name"] = "customers"
        with test_file.open('w', encoding='utf-8') as f:
            json.dump(wrong_table, f)
            
        try:
            load_checkpoint(test_file, "orders")
            print("Trường hợp 5: THẤT BẠI. Lẽ ra phải văng Exception!")
        except ValueError as e:
            print(f"Trường hợp 5 (Sai table_name)  : Pass. -> Đã chặn: {e}")
            
        # 6. Trường hợp 6: Test save_checkpoint_atomic
        if write_test_file.exists():
            write_test_file.unlink()
            
        new_checkpoint = {
            "version": 1,
            "table_name": "orders",
            "watermark": {
                "updated_at": "2026-08-05T16:15:48.000000",
                "order_id": "b29fc8cb1234567890abcdef12345678"
            }
        }
        
        # Bắt buộc truyền expected_table_name
        save_checkpoint_atomic(write_test_file, new_checkpoint, "orders")
        
        loaded_cp = load_checkpoint(write_test_file, "orders")
        if loaded_cp == new_checkpoint:
            print(f"Trường hợp 6 (Atomic Write)  : Pass. -> Dữ liệu khớp 100%.")
        else:
            print("Trường hợp 6: THẤT BẠI. Dữ liệu đọc lại không khớp với đầu vào.")
            
        if write_test_file.with_suffix('.tmp').exists():
            print("CẢNH BÁO: File .tmp chưa được dọn dẹp sau khi rename!")

    finally:
        for file_to_clean in [test_file, write_test_file, write_test_file.with_suffix('.tmp')]:
            if file_to_clean.exists():
                file_to_clean.unlink()
        print("\nHoàn tất dọn dẹp file test.")