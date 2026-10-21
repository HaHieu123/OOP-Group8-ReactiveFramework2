#pragma once

#include "sinks/IDataSink.h"

#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace iot_stream {

/**
 * @brief Sink lưu trữ các phần tử trong hàng đợi bộ nhớ RAM (In-Memory Queue).
 * 
 * Thích hợp cho:
 * - Unit tests: Thu thập toàn bộ kết quả để assert/so sánh.
 * - Benchmarking: Đo lường thông lượng mà không bị nghẽn bởi tốc độ I/O đĩa hoặc terminal.
 * - In-memory buffer: Đệm dữ liệu tạm thời phục vụ phân tích tiếp theo.
 * 
 * @tparam T Kiểu dữ liệu nhận từ luồng.
 */
template <typename T>
class MemoryQueueSink : public IDataSink<T> {
public:
    MemoryQueueSink() = default;
    ~MemoryQueueSink() override = default;

    /**
     * @brief Đẩy một phần tử mới vào cuối hàng đợi.
     */
    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);
        queue_.push_back(value);
    }

    /**
     * @brief Ghi nhận ngoại lệ phát sinh trong pipeline.
     */
    void onError(const std::exception& ex) override {
        std::lock_guard<std::mutex> lock(mtx_);
        has_error_ = true;
        error_msg_ = ex.what();
    }

    /**
     * @brief Ghi nhận tín hiệu hoàn thành luồng.
     */
    void onComplete() override {
        std::lock_guard<std::mutex> lock(mtx_);
        completed_ = true;
    }

    void flush() override {}

    /**
     * @brief Trả về bản sao snapshot của toàn bộ hàng đợi dưới dạng std::vector<T>.
     */
    std::vector<T> getQueue() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return std::vector<T>(queue_.begin(), queue_.end());
    }

    /**
     * @brief Lấy số lượng phần tử hiện có trong hàng đợi.
     */
    size_t size() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return queue_.size();
    }

    /**
     * @brief Kiểm tra hàng đợi có rỗng hay không.
     */
    bool empty() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return queue_.empty();
    }

    /**
     * @brief Xóa toàn bộ dữ liệu trong hàng đợi.
     */
    void clear() {
        std::lock_guard<std::mutex> lock(mtx_);
        queue_.clear();
    }

    /**
     * @brief Kiểm tra xem luồng đã phát tín hiệu onComplete hay chưa.
     */
    bool isCompleted() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return completed_;
    }

    /**
     * @brief Kiểm tra xem có xảy ra lỗi hay không.
     */
    bool hasError() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return has_error_;
    }

private:
    std::deque<T> queue_;
    mutable std::mutex mtx_;
    bool completed_{false};
    bool has_error_{false};
    std::string error_msg_;
};

/**
 * @brief Factory helper tạo MemoryQueueSink dạng std::shared_ptr.
 */
template <typename T>
inline std::shared_ptr<MemoryQueueSink<T>> makeMemoryQueueSink() {
    return std::make_shared<MemoryQueueSink<T>>();
}

} // namespace iot_stream

