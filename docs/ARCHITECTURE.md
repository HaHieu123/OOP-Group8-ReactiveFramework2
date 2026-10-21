# Kiến trúc Hệ thống IoT Stream Framework

Tài liệu chi tiết về kiến trúc, nguyên lý thiết kế và hướng dẫn mở rộng cho **`iot_stream_framework`** — Reactive Object-Oriented Stream Processing Pipeline Framework (C++17).

---

## 1. Tổng quan & Sơ đồ luồng dữ liệu (Dataflow)

Framework được thiết kế theo hướng module hóa cao, tách biệt rõ ràng giữa **Nguồn dữ liệu (Source)**, **Cơ chế truyền thông (Pub-Sub)**, **Điều phối áp lực (Backpressure)**, **Toán tử xử lý (Pipeline/Operators)** và **Đích nhận (Sinks)**.

### Sơ đồ luồng dữ liệu (ASCII Flow)

```text
+---------------------------------------------------------------------------------------+
|                                DATA SOURCE LAYER                                      |
|  [Sensor S1]       [Sensor S2]       [Sensor S3]      (std::thread / Periodic timer)  |
+-------+-----------------+-----------------+-------------------------------------------+
        |                 |                 |
        v                 v                 v
+-------+-----------------+-----------------+-------------------------------------------+
|                          PUBLISHER-SUBSCRIBER CORE                                    |
|                                                                                       |
|   +---------------------------------------------------+                               |
|   |                  Publisher<T>                     |                               |
|   |   - subscribe(Subscriber<T>) -> Subscription      |                               |
|   |   - emit(item), error(ex), complete()             |                               |
|   +-------------------------+-------------------------+                               |
+-----------------------------|---------------------------------------------------------+
                              v
+-----------------------------|---------------------------------------------------------+
|                    BACKPRESSURE CONTROLLER                                            |
|                                                                                       |
|   +---------------------------------------------------+                               |
|   |  Backpressure Strategy: [BUFFER | DROP | LATEST]   |                               |
|   |  Thread-Safe Bounded Queue (std::condition_var)   |                               |
|   +-------------------------+-------------------------+                               |
+-----------------------------|---------------------------------------------------------+
                              v
+-----------------------------|---------------------------------------------------------+
|                    PIPELINE & OPERATORS LAYER                                         |
|                                                                                       |
|   +---------------------------------------------------+                               |
|   |  DataStream<T>::filter(Predicate)                 |  --> Loại bỏ dữ liệu thừa     |
|   +-------------------------+-------------------------+                               |
|                             v                                                         |
|   +---------------------------------------------------+                               |
|   |  DataStream<T>::map(Transformer)                  |  --> Chuyển đổi định dạng     |
|   +-------------------------+-------------------------+                               |
|                             v                                                         |
|   +---------------------------------------------------+                               |
|   |  Window Operator (Tumbling / Sliding / Count)     |  --> Gom cụm theo chu kỳ      |
|   +-------------------------+-------------------------+                               |
+-----------------------------|---------------------------------------------------------+
                              v
+-----------------------------|---------------------------------------------------------+
|                         DATA SINKS LAYER                                              |
|                                                                                       |
|   +---------------+   +---------------+   +-------------------+   +---------------+   |
|   |  ConsoleSink  |   |   FileSink    |   |  MemoryQueueSink  |   | WebSocketSink |   |
|   |   (stdout)    |   |  (data.csv)   |   |   (In-Memory DB)  |   |  (Real-time)  |   |
|   +---------------+   +---------------+   +-------------------+   +---------------+   |
+---------------------------------------------------------------------------------------+
```

---

## 2. Publisher-Subscriber & Fluent Pipeline

### 2.1. Publisher - Subscriber Pattern
Cốt lõi reactive tuân theo chuẩn Reactive Streams Specification nhưng được hiện thực bằng C++17 thuần (`std::function`, `std::shared_ptr`, `std::weak_ptr`):
- **`Publisher<T>`**: Đối tượng nắm giữ danh sách các `Subscriber`. Khi có dữ liệu mới, `emit()` sẽ dispatch dữ liệu tới tất cả subscribers đang hoạt động.
- **`Subscriber<T>`**: Interface nhận các thông điệp:
  - `on_next(const T&)`: Nhận phần tử dữ liệu tiếp theo.
  - `on_error(const std::exception&)`: Xử lý ngoại lệ trong chuỗi pipeline.
  - `on_completed()`: Nhận tín hiệu luồng đã hoàn tất.
- **`Subscription`**: Token đại diện cho mối liên kết giữa Publisher và Subscriber, cho phép `cancel()` hoặc `unsubscribe()` an toàn thread-safe.

### 2.2. Pipeline & Fluent Interface
Lớp `DataStream<T>` cung cấp cú pháp Fluent Interface (Method Chaining) giúp code ngắn gọn, trực quan và dễ đọc:

```cpp
auto stream = sensor_source.to_stream()
    .filter([](const SensorData& d) { return d.severity >= Severity::WARN; })
    .map([](const SensorData& d) { return MetricSummary{d.id, d.temp, d.humidity}; })
    .window(TumblingWindow::of_time(std::chrono::seconds(2)))
    .to(std::make_shared<ConsoleSink>());
```

---

## 3. Windowing (Strategy Pattern)

Windowing cho phép tổng hợp (aggregation) luồng dữ liệu vô hạn thành các tập hợp con (batch) hữu hạn theo thời gian hoặc số lượng.

### 3.1. Tumbling Window (Cửa sổ cố định, không chồng lấn)
Các cửa sổ thời gian liên tiếp nhau và không giao nhau. Mỗi phần tử chỉ thuộc duy nhất về một cửa sổ.

```text
Timeline (giây): 0s        2s        4s        6s        8s
Events:          --e1--e2---|--e3-e4---|--e5-----|--e6--e7---|
Windows:         [Window 1]  [Window 2] [Window 3][Window 4]
Batch Output:    {e1, e2}    {e3, e4}   {e5}      {e6, e7}
```

### 3.2. Sliding Window (Cửa sổ trượt, có chồng lấn)
Cửa sổ có kích thước cố định $W$ nhưng trượt theo bước nhảy $S$ ($S < W$). Một phần tử có thể thuộc nhiều cửa sổ khác nhau.

```text
Window Size = 4s, Slide Step = 2s
Timeline (giây): 0s        2s        4s        6s        8s
Events:          --e1--e2---|--e3--e4--|--e5-----|--e6-------|
Window 1 [0-4s]: [    {e1, e2, e3, e4}    ]
Window 2 [2-6s]:           [    {e3, e4, e5}     ]
Window 3 [4-8s]:                     [   {e5, e6}    ]
```

### 3.3. Count Window (Cửa sổ số lượng)
Kích hoạt phát tín hiệu khi thu thập đủ $N$ phần tử, độc lập với thời gian trôi qua.

```text
Count = 3 items
Events:          --e1----e2--------e3---|-----e4---e5---e6---|---e7...
Batch Output:    [  Window 1: {e1,e2,e3} ]    [ Window 2: {e4,e5,e6} ]
```

---

## 4. Backpressure Strategy Pattern

Khi Sensor/Producer đẩy dữ liệu với tốc độ cao hơn năng lực xử lý hoặc ghi I/O của Sink, cơ chế Backpressure sẽ kích hoạt theo 4 chiến lược cấu hình:

| Chiến lược | Cơ chế xử lý | Khi nào nên sử dụng? |
| :--- | :--- | :--- |
| **`BUFFER`** | Lưu trữ vào bounded queue. Nếu queue đầy, Producer thread bị block (`std::condition_variable`) chờ Consumer giải phóng chỗ trống. | **Dữ liệu quan trọng, không được mất mát** (ví dụ: giao dịch tài chính, cảnh báo an toàn công nghiệp, audit logs). |
| **`DROP`** | Khi queue đầy, mọi bản tin mới gửi đến sẽ bị bỏ qua ngay lập tức mà không làm treo Producer. | **Dữ liệu tần số rất cao** (ví dụ: rung chấn cơ khí 1000Hz, cảm biến âm thanh) khi mất vài gói tin không ảnh hưởng xu hướng chung. |
| **`LATEST`** | Khi queue đầy, loại bỏ bản tin **cũ nhất** trong hàng đợi để nạp bản tin mới nhất vào. | **Giám sát trạng thái thời gian thực** (ví dụ: tọa độ GPS hiện tại, đồng hồ tốc độ xe) nơi trạng thái mới nhất luôn giá trị hơn trạng thái cũ. |
| **`ERROR`** | Ném ngoại lệ hoặc gọi callback `on_error()` ngay khi buffer chạm ngưỡng đầy. | **Hệ thống kiểm soát nghiêm ngặt** (fail-fast, fail-safe), cần phát hiện tắc nghẽn tức thì để kích hoạt failover. |

---

## 5. Bảng ánh xạ Design Pattern → Class

| Design Pattern | Lớp / Interface C++ | Vai trò & Trách nhiệm chính |
| :--- | :--- | :--- |
| **Publisher-Subscriber** | `Publisher<T>` | Quản lý danh sách người nhận và phát sinh sự kiện. |
| **Observer** | `Subscriber<T>` | Interface nhận callbacks `on_next`, `on_error`, `on_completed`. |
| **Pipeline** | `DataStream<T>`, `IStream<T>` | Chuỗi xử lý dòng dữ liệu dạng nối tiếp. |
| **Strategy** | `IWindowStrategy<T>` | Đóng gói thuật toán gom cụm cửa sổ (`Tumbling`, `Sliding`, `Count`). |
| **Strategy** | `IBackpressureStrategy<T>` | Đóng gói thuật toán điều tiết quá tải (`Buffer`, `Drop`, `Latest`, `Error`). |
| **Sink / Terminal** | `IDataSink<T>` | Giao diện chuẩn cho tất cả các điểm đến cuối cùng của dữ liệu. |
| **Factory / Builder** | `SensorSource`, `DataStream` | Tạo lập nguồn dữ liệu và dựng cấu hình pipeline dạng fluent. |

---

## 6. Hướng dẫn mở rộng (Extension Guide)

### 6.1. Thêm Operator mới
Tất cả operator trong `DataStream` nhận một hàm chuyển đổi hoặc đối tượng có thể gọi (`Callable`). Bạn có thể tạo thêm toán tử riêng hoặc mở rộng `DataStream`:

```cpp
// Ví dụ: Toán tử nhân đôi giá trị đo đạc
template <typename T>
auto multiply_operator(double factor) {
    return [factor](const T& item) -> T {
        T modified = item;
        modified.value *= factor;
        return modified;
    };
}

// Sử dụng trong pipeline:
stream.map(multiply_operator<SensorData>(2.0));
```

### 6.2. Thêm Sink mới
Để xuất dữ liệu ra MQTT, Kafka, Database hoặc Redis, chỉ cần kế thừa interface `IDataSink<T>`:

```cpp
#include "sinks/IDataSink.h"
#include <iostream>

template <typename T>
class CustomMqttSink : public IDataSink<T> {
public:
    explicit CustomMqttSink(const std::string& topic) : topic_(topic) {}

    void on_data(const T& item) override {
        // Gửi dữ liệu qua MQTT broker
        // mqtt_client_->publish(topic_, serialize(item));
        std::cout << "[MQTT:" << topic_ << "] Published: " << item << "\n";
    }

    void on_error(const std::exception& e) override {
        std::cerr << "[MQTT Error] " << e.what() << "\n";
    }

    void on_complete() override {
        std::cout << "[MQTT] Stream completed.\n";
    }

private:
    std::string topic_;
};
```
Sau đó kết nối vào cuối pipeline:
```cpp
stream.to(std::make_shared<CustomMqttSink<SensorData>>("sensors/telemetry"));
```

