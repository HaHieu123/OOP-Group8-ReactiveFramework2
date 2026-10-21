# IoT Stream Dashboard 📊

Module trực quan hóa thời gian thực (Real-time Visualization Dashboard) cho hệ thống **`iot_stream_framework`**. Dashboard tự động đọc và vẽ đồ thị các chỉ số telemetry cảm biến được phát sinh từ C++ streaming engine qua file CSV.

---

## 🚀 Cài đặt & Khởi chạy

### 1. Cài đặt thư viện phụ thuộc
Đảm bảo bạn đã cài đặt Python 3.9 trở lên:

```bash
pip install -r requirements.txt
```

### 2. Khởi chạy Dashboard
Khởi chạy dashboard để theo dõi file dữ liệu stream:

```bash
python dashboard.py --file data.csv --refresh 1
```

- `--file`: Đường dẫn tới file CSV do C++ `FileSink` ghi ra (mặc định: `data.csv`).
- `--refresh`: Chu kỳ làm mới dữ liệu trên biểu đồ tính bằng giây (mặc định: `1`).

---

## 📋 Schema dữ liệu (`data.csv`)

File CSV được sinh ra theo thời gian thực (runtime generated) từ C++ pipeline với cấu trúc định dạng:

```csv
timestamp,sensor_id,temperature,humidity,severity
```

### Chi tiết các trường dữ liệu:
| Trường | Kiểu dữ liệu | Mô tả | Ví dụ |
| :--- | :--- | :--- | :--- |
| `timestamp` | `int64` / ISO8601 | Thời gian ghi nhận mẫu dữ liệu (Epoch milliseconds hoặc chuỗi thời gian) | `1700000000123` |
| `sensor_id` | `string` | Mã định danh của cảm biến IoT | `S1`, `S2`, `S3` |
| `temperature` | `float` | Giá trị nhiệt độ môi trường đo được (°C) | `42.5` |
| `humidity` | `float` | Độ ẩm tương đối không khí (%) | `68.2` |
| `severity` | `string` / `int` | Mức độ cảnh báo: `INFO`, `WARN`, `ERROR`, `CRITICAL` | `WARN` |

---

## 🖼️ Giao diện Dashboard (Screenshot)

```text
+---------------------------------------------------------------------------------+
|                         IoT Real-Time Telemetry Monitor                         |
|  Status: ACTIVE | File: data.csv | Refresh: 1000ms | Total Records: 1,420       |
+---------------------------------------------------------------------------------+
|  [ Temperature Chart (°C) ]                    [ Humidity Chart (%) ]           |
|   50 |         /\                               80 |      _/\_                  |
|   40 |    _/\_/  \__                            60 |   __/    \__               |
|   30 |___/          \___                        40 |__/        \___             |
|      +------------------->                         +------------------->        |
|       0s    2s    4s    6s                          0s    2s    4s    6s        |
+---------------------------------------------------------------------------------+
|  [ Recent Severity Events Log ]                                                 |
|  [10:15:32] [WARN]     Sensor S1: Temp exceeded threshold (43.2°C)              |
|  [10:15:34] [CRITICAL] Sensor S2: Temperature spike (61.5°C) -> Overheat Alert! |
|  [10:15:35] [INFO]     Sensor S3: Normal telemetry pulse received               |
+---------------------------------------------------------------------------------+
```

> *(Placeholder for UI screenshot: `assets/dashboard_screenshot.png`)*

