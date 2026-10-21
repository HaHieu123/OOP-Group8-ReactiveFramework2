#pragma once

#include "core/Exceptions.h"
#include "core/Subscriber.h"
#include "core/Subscription.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace iot_stream {

/**
 * @brief Các chiến lược điều tiết áp lực ngược (Backpressure Strategies).
 * 
 * Tại sao cần Backpressure trong hệ thống IoT Streaming?
 * - Trong các hệ thống IoT, tốc độ phát tín hiệu của nguồn (Producer - ví dụ: cảm biến rung chấn,
 *   telemetry 1000Hz) thường lớn hơn rất nhiều năng lực xử lý hoặc ghi I/O của đích (Consumer -
 *   ví dụ: ghi cơ sở dữ liệu, gửi qua mạng 4G/LTE, vẽ UI 10Hz).
 * - Nếu không có Backpressure, các bản tin chưa được xử lý sẽ dồn ứ vô hạn trong RAM, dẫn đến
 *   cạn kiệt bộ nhớ (OOM Crash) hoặc làm tăng độ trễ (latency drift) khiến dữ liệu bị trễ thời gian thực.
 */
enum class BackpressureStrategy {
    BUFFER,  ///< Lưu trữ trong hàng đợi bounded ring buffer. Đầy buffer thì drop bản tin mới.
    DROP,    ///< Bỏ qua ngay lập tức các bản tin mới khi Consumer chưa sẵn sàng tiêu thụ (demand == 0).
    LATEST,  ///< Chỉ giữ lại bản tin mới nhất, ghi đè các bản tin cũ hơn chưa kịp xử lý.
    ERROR    ///< Ném BackpressureOverflowException ngay khi xảy ra quá tải (Fail-Fast).
};

/**
 * @brief Subscription tích hợp cơ chế kiểm soát áp lực ngược theo Reactive Streams Specification.
 * 
 * @tparam T Kiểu dữ liệu phần tử luồng.
 */
template <typename T>
class BackpressureSubscription : public Subscription {
public:
    /**
     * @brief Khởi tạo BackpressureSubscription.
     * @param strategy Chiến lược áp dụng (BUFFER, DROP, LATEST, ERROR).
     * @param capacity Dung lượng tối đa của hàng đợi (chỉ áp dụng cho BUFFER).
     * @param downstream Con trỏ tới Subscriber tiếp nhận dữ liệu.
     */
    BackpressureSubscription(BackpressureStrategy strategy, size_t capacity, Subscriber<T>* downstream)
        : strategy_(strategy),
          capacity_(capacity),
          downstream_(downstream) {}

    ~BackpressureSubscription() override = default;

    /**
     * @brief Subscriber thông báo nhu cầu sẵn sàng tiếp nhận thêm n phần tử dữ liệu.
     * Tăng demand và xả (flush) dữ liệu đang tồn đọng trong buffer/latest xuống downstream.
     * 
     * @param n Số lượng phần tử yêu cầu thêm.
     */
    void request(int n) override;

    /**
     * @brief Hủy subscription, làm sạch buffer và dừng mọi luồng phát.
     */
    void cancel() override;

    /**
     * @brief Xử lý phần tử dữ liệu mới từ Producer theo chiến lược Backpressure cấu hình.
     * @param value Giá trị sự kiện mới.
     */
    void onNext(const T& value);

    /**
     * @brief Nhận thông báo hoàn thành từ Producer.
     */
    void onComplete();

    /**
     * @brief Nhận thông báo lỗi từ Producer.
     */
    void onError(const std::exception& ex);

    /**
     * @brief Số lượng phần tử đã bị loại bỏ (drop) do quá tải.
     */
    size_t droppedCount() const {
        return droppedCount_.load();
    }

    /**
     * @brief Số lượng phần tử hiện đang nằm chờ trong hàng đợi buffer hoặc latest slot.
     */
    size_t bufferedCount() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return buffer_.size() + (latest_.has_value() ? 1 : 0);
    }

    /**
     * @brief Lấy số lượng demand hiện tại.
     */
    int demand() const {
        return demand_.load();
    }

private:
    BackpressureStrategy strategy_;
    size_t capacity_;
    std::deque<T> buffer_;
    std::optional<T> latest_;
    std::atomic<int> demand_{0};
    Subscriber<T>* downstream_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::atomic<size_t> droppedCount_{0};
    bool cancelled_{false};
    bool completed_{false};
};

/**
 * @brief Factory helper tạo BackpressureSubscription dạng std::shared_ptr.
 */
template <typename T>
inline std::shared_ptr<BackpressureSubscription<T>> makeBackpressureSubscription(
    BackpressureStrategy strategy,
    size_t capacity,
    Subscriber<T>* downstream) {
    return std::make_shared<BackpressureSubscription<T>>(strategy, capacity, downstream);
}

} // namespace iot_stream

#include "backpressure/Backpressure.tpp"

/*
================================================================================
ASCII TIMELINE MINH HỌA CÁC CHIẾN LƯỢC BACKPRESSURE
Kịch bản: Producer phát 1000 events/s (1ms/event), Consumer xử lý 10 events/s (100ms/event)
================================================================================

1. BUFFER Strategy (Capacity = 5):
Timeline (ms): 0ms    1ms    2ms    3ms    4ms    5ms    6ms ... 100ms
Producer:      e1     e2     e3     e4     e5     e6     e7  ... e100
Consumer:      e1 (đang xử lý bận rộn tới 100ms)
Buffer:        [-]    [e2]   [e2,e3][e2..e4][e2..e5][e2..e6] Buffer Đầy (cap=5)
               -> Từ e7 trở đi: DROP và tăng droppedCount cho tới khi consumer request tiếp.

2. DROP Strategy:
Timeline (ms): 0ms    1ms    2ms    3ms ... 100ms
Producer:      e1     e2     e3     e4  ... e100
Consumer:      e1 (đang xử lý bận rộn)
Hành vi:       e1 được gửi ngay. Toàn bộ e2..e100 (99 events) bị DROP ngay lập tức vì demand == 0.
               droppedCount = 99.

3. LATEST Strategy:
Timeline (ms): 0ms    1ms    2ms    3ms ... 100ms
Producer:      e1     e2     e3     e4  ... e100
Consumer:      e1 (đang xử lý bận rộn)
Latest Slot:   [-]    [e2]   [e3]   [e4]... [e100]  (e2..e99 bị ghi đè, droppedCount = 98)
Consumer 100ms: Xử lý xong e1 -> request(1) -> Nhận ngay [e100] (giá trị đo đạc mới nhất!).

4. ERROR Strategy:
Timeline (ms): 0ms    1ms
Producer:      e1     e2
Hành vi:       Ngay khi e2 gửi đến mà demand == 0:
               Downstream nhận BackpressureOverflowException. Luồng lập tức bị ngắt (Fail-Fast).

================================================================================
BẢNG SO SÁNH 4 CHIẾN LƯỢC BACKPRESSURE:
================================================================================
| Chiến lược  | Tiêu tốn RAM  | Độ tươi (Freshness) | Mất dữ liệu (Loss) | Trường hợp áp dụng tiêu biểu             |
| :---------- | :------------ | :------------------ | :----------------- | :--------------------------------------- |
| BUFFER      | Vừa (theo cap)| Trung bình (FIFO)   | Hạn chế mất         | Dữ liệu quan trọng: log tài chính, alarm |
| DROP        | Rất thấp O(1) | Trung bình          | Có (bỏ mẫu mới)    | Cảm biến âm thanh, dao động tần số cao    |
| LATEST      | Rất thấp O(1) | Cao nhất (Fresh)    | Có (bỏ mẫu cũ)     | GPS vị trí, tốc độ xe, dashboard gauge    |
| ERROR       | Rất thấp      | N/A                 | N/A (dừng luồng)   | Kiểm thử tải, phát hiện tắc nghẽn nghiêm ngặt |
================================================================================
*/

