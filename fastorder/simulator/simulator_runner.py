from fastorder.simulator.order_generator import generate_single_order
from fastorder.simulator.order_status_updater import order_status_updater
import time
import random 
import sys

def runner(): 
    print("Runner đang chạy, dữ liêu đang được mô phỏng ...")
    print("Nhấn Ctrl + C để dừng mô phỏng")

    try:
        while True:
            try:
                if random.random() < 0.5:
                    print("Có đơn hàng mới")
                    generate_single_order()
                else:
                    print("Nhịp này không có đơn hàng mới")

                print("Thực hiện quét và cập nhật trạng thái đơn hàng")
                order_status_updater()

                sleep_time = random.randint(30, 60)
                print(f"Chờ {sleep_time} giây trước khi quét đơn hàng tiếp theo ...")
                time.sleep(sleep_time)
            except Exception as e:
                print(f"Đã xảy ra lỗi: {e}")
                print("Hệ thống thử lại sau 10 giây ...")
                time.sleep(10)
    except KeyboardInterrupt:
        print("Nhận tính hiệu dừng runner. Mô phỏng kết thúc")
        sys.exit(0)

def run_cycles(n_cycles): 
    print("Runner đang chạy, dữ liêu đang được mô phỏng ...")
    print("Nhấn Ctrl + C để dừng mô phỏng")

    try:
        for cycle in range(1, n_cycles + 1):
            try:
                if random.random() < 0.5:
                    print("Có đơn hàng mới")
                    generate_single_order()
                else:
                    print("Nhịp này không có đơn hàng mới")

                print("Thực hiện quét và cập nhật trạng thái đơn hàng")
                order_status_updater()

                sleep_time = random.randint(30, 60)
                print(f"Chờ {sleep_time} giây trước khi quét đơn hàng tiếp theo ...")
                time.sleep(sleep_time)
            except Exception as e:
                print(f"Đã xảy ra lỗi: {e}")
                print("Hệ thống thử lại sau 10 giây ...")
                time.sleep(10)
    except KeyboardInterrupt:
        print("Nhận tính hiệu dừng runner. Mô phỏng kết thúc")
        sys.exit(0)

if __name__ == "__main__":
    print("Chon chế độ chạy:")
    print("1. Chạy liên tục")
    print("2. Chạy theo số chu kỳ")
    choice = input("Nhập lựa chọn của bạn: ")
    if choice == "1":
        runner()
    elif choice == "2":
        n_cycles = int(input("Nhập số chu kỳ muốn chạy: "))
        run_cycles(n_cycles)
        
