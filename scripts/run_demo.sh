#!/usr/bin/env bash
# ==============================================================================
# Script tự động Build & Chạy Demo toàn bộ hệ sinh thái iot_stream_framework
# Cách cấp quyền thực thi trước khi chạy:
#   chmod +x scripts/run_demo.sh
# Cách chạy:
#   ./scripts/run_demo.sh
# ==============================================================================

set -e

# Đảm bảo làm việc tại thư mục gốc của dự án
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "======================================================================"
echo "  [1/4] Cấu hình và Biên dịch C++17 Streaming Pipeline Framework"
echo "======================================================================"
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"

# Xác định vị trí file thực thi iot_app
APP_BIN=""
if [ -f "./build/iot_app" ]; then
    APP_BIN="./build/iot_app"
elif [ -f "./build/bin/iot_app" ]; then
    APP_BIN="./build/bin/iot_app"
elif [ -f "./build/bin/Release/iot_app" ]; then
    APP_BIN="./build/bin/Release/iot_app"
elif [ -f "./build/iot_app.exe" ]; then
    APP_BIN="./build/iot_app.exe"
elif [ -f "./build/bin/Release/iot_app.exe" ]; then
    APP_BIN="./build/bin/Release/iot_app.exe"
else
    echo "[Lỗi] Không tìm thấy binary iot_app đã biên dịch!" >&2
    exit 1
fi

echo "======================================================================"
echo "  [2/4] Khởi động C++ Engine ($APP_BIN) ở chế độ nền"
echo "======================================================================"
# Biến lưu trữ PID của tiến trình C++ app
APP_PID=""

# Thiết lập trap để dọn dẹp tiến trình nền khi người dùng bấm Ctrl+C hoặc thoát script
cleanup() {
    echo -e "\n[Thông báo] Đang dừng tiến trình C++ Engine (PID: $APP_PID)..."
    if [ -n "$APP_PID" ] && kill -0 "$APP_PID" 2>/dev/null; then
        kill "$APP_PID" 2>/dev/null || true
        wait "$APP_PID" 2>/dev/null || true
    fi
    echo "[Hoàn tất] Đã dừng toàn bộ dịch vụ an toàn."
}
trap cleanup EXIT INT TERM

# Khởi chạy app streaming trong nền
"$APP_BIN" &
APP_PID=$!
echo "[Khởi tạo] C++ Stream App đang chạy với PID: $APP_PID"

echo "======================================================================"
echo "  [3/4] Chờ luồng dữ liệu sẵn sàng (1s)..."
echo "======================================================================"
sleep 1

echo "======================================================================"
echo "  [4/4] Khởi chạy Dashboard hiển thị thời gian thực (Python)"
echo "======================================================================"
cd dashboard

# Kiểm tra python3 hoặc python
PYTHON_CMD="python3"
if ! command -v python3 &>/dev/null; then
    PYTHON_CMD="python"
fi

if [ -f "dashboard.py" ]; then
    $PYTHON_CMD dashboard.py --file data.csv
else
    echo "[Cảnh báo] dashboard/dashboard.py chưa được tạo. Bỏ qua bước mở dashboard."
    echo "[Thông tin] C++ Engine vẫn đang chạy. Nhấn Ctrl+C để thoát."
    wait "$APP_PID"
fi

