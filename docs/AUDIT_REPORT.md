# 🔍 AUDIT REPORT — IoT Stream Framework

## Ngày audit: 2026-10-21
## Môi trường: Windows 11 (x64), MSVC 2022 (v14.44), CMake 4.4.3 / 3.16+

---

## 1. Thống Kê Tổng Quan
- **Tổng số files mã nguồn kiểm tra**: 61 files (bao gồm toàn bộ 40 files trọng tâm kiến trúc, test suite, benchmark, dashboard, scripts, docs, UML).
- **Files đạt chuẩn ban đầu (OK)**: 56 files
- **Files phát hiện lỗi & đã sửa (Fixed)**: 5 files
  - `benchmark/benchmark.cpp`
  - `tests/test_window.cpp`
  - `tests/test_integration.cpp`
  - `src/main.cpp`
  - `src/backpressure/Backpressure.cpp`

---

## 2. Chi Tiết Lỗi Đã Khắc Phục

### Lỗi 1 — `benchmark/benchmark.cpp`: Include path sai & Xung đột Windows Macro
- **Hiện tượng**: `error C1083: Cannot open include file: 'iot_stream/iot_stream.h'`; sau đó xung đột `error C2589: 'constant': illegal token on right side of '::'` do `windows.h` define macro `ERROR 0`.
- **Nguyên nhân**: Include đường dẫn ô dù không tồn tại thay vì include phẳng; header Windows định nghĩa macro `ERROR` đè lên enum `BackpressureStrategy::ERROR`.
- **Khắc phục**:
  - Chuyển toàn bộ sang include phẳng: `"core/DataStream.h"`, `"backpressure/Backpressure.h"`, `"operators/Window.h"`, v.v.
  - Thêm `#define NOMINMAX`, `#define WIN32_LEAN_AND_MEAN`, `#undef ERROR` trước khi sử dụng `BackpressureStrategy::ERROR`.
  - Hiện thực đầy đủ 3 kịch bản: Throughput (1,000,000 events), Latency (p50/p95/p99), và bảng so sánh 4 chiến lược Backpressure (BUFFER, DROP, LATEST, ERROR) khi Producer nhanh gấp 10 lần Consumer.

### Lỗi 2 — `tests/test_window.cpp`: Duplicate khối test & Dòng mã rác
- **Hiện tượng**: Lỗi biên dịch `error C4430`, `error C2365: assert_near_impl redefinition`, `error C2374: _test_reg_Window_Stats_EmptyWindow redefinition`.
- **Nguyên nhân**: File bị dán trùng 2 lần các test `Stats_EmptyWindow`, `Stats_MinMaxThrowsOnEmpty`, kèm đoạn mã rác `win, 0);` ngoài hàm test.
- **Khắc phục**:
  - Viết lại toàn bộ file sạch sẽ, chuẩn xác theo 10 test cases bắt buộc:
    * `TEST(Window_Tumbling_Basic)`
    * `TEST(Window_Sliding_Basic)`
    * `TEST(Window_Count_Basic)`
    * `TEST(Window_Stats_Sum)`
    * `TEST(Window_Stats_Avg)`
    * `TEST(Window_Stats_Count)`
    * `TEST(Window_Stats_MinMax)`
    * `TEST(Window_Stats_EmptyWindow)`
    * `TEST(Window_Stats_MinMaxThrowsOnEmpty)`
    * `TEST(Window_Stats_VarianceStddev)`
  - Cập nhật macro `TEST` trong `tests/test_framework.h` hỗ trợ cả 1 tham số `TEST(Name)` lẫn 2 tham số `TEST(Suite, Name)`.

### Lỗi 3 — `tests/test_integration.cpp`: Scope DataStream bị hủy sớm
- **Hiện tượng**: `FilterWARN_ReducesCount` và `Pipeline_MapToString` nhận được 0 phần tử tại Sink (kỳ vọng 2 phần tử).
- **Nguyên nhân**: `DataStream<SensorReading>(source).filter(...).subscribe(sub)` tạo đối tượng `DataStream` tạm thời; sau biểu thức đó, `FilterPublisher` bị hủy, dẫn đến `FilterSubscription` bị giải phóng và để lại con trỏ thô dangling trong `SimplePublisher`.
- **Khắc phục**: Giữ `auto stream = DataStream<SensorReading>(source).filter(...);` trong scope của test case để vòng đời pipeline tồn tại suốt thời gian phát sự kiện.

### Lỗi 4 — `src/main.cpp`: DataStream Scope & Font tiếng Việt Windows
- **Hiện tượng**: `iot_app.exe` thoát với mã lỗi hoặc treo pipeline; font tiếng Việt trong console bị lỗi ký tự.
- **Nguyên nhân**: 2 pipeline được bọc trong block scope `{ auto stream = ...; }` nên đã bị hủy trước khi `source->start()` bắt đầu phát; thiếu cấu hình codepage UTF-8 cho Windows console.
- **Khắc phục**:
  - Kéo `stream1` và `stream2` ra ngoài phạm vi khối lệnh `try`.
  - Thêm `SetConsoleOutputCP(CP_UTF8)` tại dòng đầu của `main()`.

### Lỗi 5 — `src/backpressure/Backpressure.cpp`: Thiếu Explicit Template Instantiation
- **Hiện tượng**: Tiềm ẩn lỗi link `LNK2019` unresolved external symbol khi sử dụng `BackpressureSubscription<SensorReading>`.
- **Khắc phục**: Bổ sung `#include "source/Sensor.h"` và `template class BackpressureSubscription<SensorReading>;` vào cuối file.

---

## 3. Cảnh Báo Tiềm Ẩn & Khuyến Nghị

1. **Vòng đời của `DataStream`**:
   - Khi sử dụng phương thức `.subscribe(Subscriber*)`, đối tượng `DataStream` nắm giữ các intermediate publisher phải được lưu vào biến (`auto stream = ...`) trong cùng scope với nguồn phát.
   - Khi sử dụng phương thức `.to(shared_ptr<IDataSink>)`, `LambdaSubscriber` tự động neo giữ pipeline (`pipelineAnchor_`) nên có thể gọi trực tiếp theo chuỗi Fluent API.
2. **Hệ thống tệp đầu ra `dashboard/data.csv`**:
   - Ứng dụng `iot_app.exe` ghi các cảnh báo window vào file `dashboard/data.csv`. File này sẵn sàng để script `dashboard/dashboard.py` đọc và vẽ biểu đồ realtime.
3. **Chạy Benchmark**:
   - Khuyến nghị chạy benchmark trên cấu hình `Release` để bộ tối ưu hóa `/O2` của MSVC đạt hiệu quả tối đa (~6.8 triệu events/giây).

---

## 4. Kết Quả Kiểm Thử Cuối Cùng
- **Unit Tests (`test_all.exe`)**: 43 / 43 tests PASS (100%).
- **Benchmark (`run_benchmarks.exe`)**: Throughput đạt **6,854,898 events/giây**, độ trễ trung vị p50 = **0.100 µs**.
- **IoT App Demo (`iot_app.exe`)**: Hoạt động hoàn hảo 10s, phát 276 sự kiện, tổng hợp và xuất file `dashboard/data.csv` thành công.

