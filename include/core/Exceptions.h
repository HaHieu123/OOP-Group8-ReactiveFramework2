#pragma once

#include <stdexcept>
#include <string>

namespace iot_stream {

/**
 * @brief Lớp ngoại lệ cơ sở (Base Exception) cho toàn bộ hệ thống Reactive Stream.
 * 
 * Khi nào bắt: Bắt ngoại lệ này khi muốn xử lý chung tất cả các lỗi bắt nguồn
 * từ tầng xử lý luồng (Streaming Pipeline).
 */
class StreamException : public std::runtime_error {
public:
    explicit StreamException(const std::string& msg) : std::runtime_error(msg) {}
};

/**
 * @brief Ngoại lệ phát sinh khi hàng đợi luồng bị quá tải (Buffer Overflow).
 * 
 * Khi nào ném (throw):
 * - Kích hoạt khi chiến lược Backpressure được cấu hình là `ERROR` và số lượng
 *   phần tử vượt quá ngưỡng `capacity` cho phép.
 * - Producer đẩy dữ liệu quá nhanh trong khi hàng đợi có giới hạn bị tràn đầy.
 */
class BackpressureOverflowException : public StreamException {
public:
    explicit BackpressureOverflowException(const std::string& msg)
        : StreamException("BackpressureOverflowException: " + msg) {}
};

/**
 * @brief Ngoại lệ phát sinh khi thực hiện thao tác trên luồng đã bị đóng hoặc bị hủy.
 * 
 * Khi nào ném (throw):
 * - Khi gọi emit/onNext, request, hoặc thao tác dữ liệu trên một Stream/Publisher/Subscription
 *   đã hoàn thành (onComplete), đã gặp lỗi (onError), hoặc đã bị hủy (cancel).
 */
class StreamClosedException : public StreamException {
public:
    explicit StreamClosedException(const std::string& msg)
        : StreamException("StreamClosedException: " + msg) {}
};

/**
 * @brief Ngoại lệ phát sinh khi thực hiện thao tác không hợp lệ đối với trạng thái hiện tại.
 * 
 * Khi nào ném (throw):
 * - Khi parse chuỗi sang enum Severity thất bại (chuỗi không khớp với DEBUG, INFO, WARN, ERROR, FATAL).
 * - Khi gọi subscription->request(n) với n <= 0 vi phạm Reactive Streams Contract.
 * - Khi pipeline ở trạng thái cấu hình không tương thích hoặc chưa được khởi tạo đúng cách.
 */
class InvalidOperationException : public StreamException {
public:
    explicit InvalidOperationException(const std::string& msg)
        : StreamException("InvalidOperationException: " + msg) {}
};

} // namespace iot_stream

