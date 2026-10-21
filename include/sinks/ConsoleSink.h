#pragma once

#include "sinks/IDataSink.h"
#include "source/Sensor.h"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>

namespace iot_stream {

/**
 * @brief Sink xuất dữ liệu trực tiếp ra màn hình console (stdout / terminal).
 * 
 * Đặc điểm:
 * - Thread-safe: Sử dụng std::mutex bảo vệ std::cout, tránh hiện tượng các dòng log bị đan xen
 *   khi nhiều luồng cùng phát dữ liệu.
 * - Hỗ trợ hiển thị nhãn thời gian [YYYY-MM-DD HH:MM:SS].
 * - Định dạng thông minh: Tự động nhận diện cấu trúc SensorReading để in đẹp mắt kèm Severity,
 *   hoặc sử dụng operator<< cho các kiểu dữ liệu generic khác.
 * 
 * @tparam T Kiểu dữ liệu nhận từ luồng.
 */
template <typename T>
class ConsoleSink : public IDataSink<T> {
public:
    /**
     * @brief Khởi tạo ConsoleSink.
     * @param showTimestamp Tùy chọn hiển thị timestamp (mặc định = true).
     */
    explicit ConsoleSink(std::optional<bool> showTimestamp = true)
        : show_timestamp_(showTimestamp.value_or(true)) {}

    ~ConsoleSink() override {
        flush();
    }

    /**
     * @brief In phần tử dữ liệu ra màn hình console.
     */
    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);

        if constexpr (std::is_same_v<T, SensorReading>) {
            if (show_timestamp_) {
                std::cout << "[" << current_time_str() << "] ";
            }
            std::cout << "[" << to_string(value.severity) << "] "
                      << "Sensor: " << value.sensorId
                      << " | Temp: " << std::fixed << std::setprecision(1) << value.temperature << "°C"
                      << " | Hum: " << value.humidity << "%\n";
        } else {
            if (show_timestamp_) {
                std::cout << "[" << current_time_str() << "] ";
            }
            std::cout << value << "\n";
        }
    }

    /**
     * @brief In thông báo lỗi ra stderr / stdout.
     */
    void onError(const std::exception& ex) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (show_timestamp_) {
            std::cerr << "[" << current_time_str() << "] ";
        }
        std::cerr << "[ERROR] Pipeline exception: " << ex.what() << "\n";
    }

    /**
     * @brief In thông báo hoàn thành luồng.
     */
    void onComplete() override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (show_timestamp_) {
            std::cout << "[" << current_time_str() << "] ";
        }
        std::cout << "[COMPLETE] Stream finished processing all items.\n";
        std::cout.flush();
    }

    /**
     * @brief Đẩy toàn bộ bộ đệm stdout.
     */
    void flush() override {
        std::lock_guard<std::mutex> lock(mtx_);
        std::cout.flush();
        std::cerr.flush();
    }

private:
    bool show_timestamp_{true};
    mutable std::mutex mtx_;

    std::string current_time_str() const {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        std::tm tm_buf{};
#if defined(_WIN32) || defined(_WIN64)
        localtime_s(&tm_buf, &in_time_t);
#else
        localtime_r(&in_time_t, &tm_buf);
#endif
        std::ostringstream oss;
        oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }
};

/**
 * @brief Factory helper tạo ConsoleSink dạng std::shared_ptr.
 */
template <typename T>
inline std::shared_ptr<ConsoleSink<T>> makeConsoleSink(std::optional<bool> showTimestamp = true) {
    return std::make_shared<ConsoleSink<T>>(showTimestamp);
}

} // namespace iot_stream

