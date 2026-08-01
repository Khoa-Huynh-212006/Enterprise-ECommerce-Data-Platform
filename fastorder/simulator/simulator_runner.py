import time
import random
import sys
from typing import Dict, Any, Optional
from fastorder.simulator.order_generator import generate_single_order
from fastorder.simulator.order_status_updater import order_status_updater
from fastorder.simulator.validate_simulator import validate_simulator

def run_simulator(
    cycles: Optional[int] = None,
    min_orders_per_cycle: int = 0,
    max_orders_per_cycle: int = 2,
    sleep_seconds: int = 30
) -> Dict[str, Any]:
    mode_name = "LIÊN TỤC (Vô hạn)" if cycles is None else f"GIỚI HẠN ({cycles} chu kỳ)"
    print(f"Bắt đầu chạy Simulator - Chế độ: {mode_name}")
    print(f"Mỗi chu kỳ nghỉ {sleep_seconds}s. Nhấn Ctrl+C để dừng thủ công.\n")

    runner_summary = {
        "total_cycles_completed": 0,
        "total_orders_created": 0,
        "updater_summary": {
            "total_selected": 0,
            "total_updated": 0,
            "transitions": {}
        }
    }

    cycle_count = 1
    try:
        while True:
            if cycles is not None and cycle_count > cycles:
                break

            prefix = f"Vô hạn - Chu kỳ {cycle_count}" if cycles is None else f"Chu kỳ {cycle_count}/{cycles}"
            print(f"{prefix}")
            

            orders_to_create = random.randint(min_orders_per_cycle, max_orders_per_cycle)
            if orders_to_create > 0:
                print(f"Đang tạo {orders_to_create} đơn hàng mới...")
                for _ in range(orders_to_create):
                    generate_single_order()
                    runner_summary["total_orders_created"] += 1
            else:
                print("Nhịp này không tạo đơn hàng mới.")


            print("Đang quét và cập nhật trạng thái...")
            updater_result = order_status_updater()
            
            runner_summary["updater_summary"]["total_selected"] += updater_result.get("selected_orders", 0)
            runner_summary["updater_summary"]["total_updated"] += updater_result.get("updated_orders", 0)
            
            for transition, count in updater_result.get("transition_counts", {}).items():
                runner_summary["updater_summary"]["transitions"][transition] = \
                    runner_summary["updater_summary"]["transitions"].get(transition, 0) + count

            runner_summary["total_cycles_completed"] += 1

            if cycles is None or cycle_count < cycles:
                print(f"Tạm nghỉ {sleep_seconds} giây trước chu kỳ tiếp theo...")
                time.sleep(sleep_seconds)
            
            cycle_count += 1

    except KeyboardInterrupt:
        print("\nNhận tín hiệu dừng từ người dùng. Kết thúc sớm chuỗi mô phỏng.")

    
    print("\n" + "="*50)
    print("BÁO CÁO KẾT QUẢ MÔ PHỎNG (RUNNER SUMMARY)")
    print(f" - Số chu kỳ hoàn thành: {runner_summary['total_cycles_completed']}")
    print(f" - Tổng đơn hàng sinh mới: {runner_summary['total_orders_created']}")
    print(f" - Tổng đơn hàng được duyệt quét: {runner_summary['updater_summary']['total_selected']}")
    print(f" - Tổng đơn hàng luân chuyển thành công: {runner_summary['updater_summary']['total_updated']}")
    print(" - Chi tiết đường đi trạng thái:")
    
    if not runner_summary['updater_summary']['transitions']:
        print("    * (Chưa có trạng thái nào thay đổi)")
    else:
        for transition, count in runner_summary['updater_summary']['transitions'].items():
            print(f"    * {transition}: {count}")
    print("="*50)

    print("\nBắt đầu kiểm tra tính hợp lệ của dữ liệu mô phỏng...")
    validate_simulator()

    return runner_summary


if __name__ == "__main__":
    print("CHỌN CHẾ ĐỘ CHẠY SIMULATOR:")
    print("1. Chạy liên tục (Vô hạn - Dừng bằng Ctrl+C)")
    print("2. Chạy theo số chu kỳ giới hạn")
    
    choice = input("\nNhập lựa chọn của bạn (1 hoặc 2): ").strip()
    
    if choice == "1":
        run_simulator(cycles=None, min_orders_per_cycle=0, max_orders_per_cycle=3, sleep_seconds=30)
    elif choice == "2":
        try:
            input_cycles = input("Nhập số chu kỳ muốn chạy: ")
            cycles = int(input_cycles)
            if cycles <= 0:
                print("Số chu kỳ phải lớn hơn 0.")
                sys.exit(1)
            run_simulator(cycles=cycles, min_orders_per_cycle=0, max_orders_per_cycle=3, sleep_seconds=30)
        except ValueError:
            print("Đầu vào không hợp lệ. Vui lòng nhập một số nguyên dương.")
            sys.exit(1)
    else:
        print("Lựa chọn không hợp lệ. Hệ thống thoát.")
        sys.exit(1)