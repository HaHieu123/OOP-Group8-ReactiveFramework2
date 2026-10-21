#pragma once

#include "sinks/IDataSink.h"
#include "source/Sensor.h"

#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>

namespace iot_stream {

/**
 * @brief Sink truyền phát dữ liệu thời gian thực qua giao thức WebSocket (STUB Implementation).
 * 
 * Lưu ý kiến trúc (STUB):
 * - Đây là bản cài đặt giả lập (Mock/Stub) thông qua std::function callback để kiểm thử pipeline
 *   và mô phỏng việc đẩy dữ liệu lên Web Dashboard/Clients.
 * - Để chạy WebSocket thực tế trên môi trường Production, có thể tích hợp thư viện uWebSockets
 *   hoặc libwebsockets vào lớp này mà không làm ảnh hưởng tới các tầng pipeline phía trên.
 * 
 * @tparam T Kiểu dữ liệu nhận từ luồng.
 */
template <typename T>
class WebSocketSink : public IDataSink<T> {
public:
    using MessageCallback = std::function<void(const std::string&)>;

    /**
     * @brief Khởi tạo WebSocketSink với một hàm callback nhận thông điệp JSON.
     * @param callback Hàm callback được kích hoạt mỗi khi có khung tin (frame) JSON mới.
     */
    explicit WebSocketSink(MessageCallback callback)
        : callback_(std::move(callback)) {}

    ~WebSocketSink() override = default;

    /**
     * @brief Serialize dữ liệu sang chuỗi JSON và gọi callback truyền tin.
     */
    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!callback_) return;

        std::string json_payload;
        if constexpr (std::is_same_v<T, SensorReading>) {
            json_payload = to_json_str(value);
        } else {
            std::ostringstream oss;
            oss << "{\"type\":\"data\",\"payload\":\"" << value << "\"}";
            json_payload = oss.str();
        }

        callback_(json_payload);
    }

    /**
     * @brief Phát thông báo lỗi dạng JSON qua WebSocket.
     */
    void onError(const std::exception& ex) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!callback_) return;

        std::ostringstream oss;
        oss << "{\"type\":\"error\",\"message\":\"" << ex.what() << "\"}";
        callback_(oss.str());
    }

    /**
     * @brief Phát thông báo đóng luồng qua WebSocket.
     */
    void onComplete() override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!callback_) return;

        callback_("{\"type\":\"complete\",\"status\":\"closed\"}");
    }

    void flush() override {}

private:
    MessageCallback callback_;
    mutable std::mutex mtx_;
};

/**
 * @brief Factory helper tạo WebSocketSink dạng std::shared_ptr.
 */
template <typename T>
inline std::shared_ptr<WebSocketSink<T>> makeWebSocketSink(std::function<void(const std::string&)> callback) {
    return std::make_shared<WebSocketSink<T>>(std::move(callback));
}

} // namespace iot_stream

