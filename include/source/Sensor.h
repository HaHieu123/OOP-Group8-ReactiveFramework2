#pragma once

#include "core/Severity.h"

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace iot_stream {

/**
 * @brief Cấu trúc dữ liệu đo đạc cảm biến IoT (Sensor Telemetry Reading).
 */
struct SensorReading {
    std::string sensorId;                                                   ///< Mã định danh cảm biến (ví dụ: "S1", "S2")
    double temperature{0.0};                                               ///< Nhiệt độ môi trường (°C)
    double humidity{0.0};                                                  ///< Độ ẩm tương đối (%)
    std::chrono::system_clock::time_point timestamp{std::chrono::system_clock::now()}; ///< Thời điểm đo
    Severity severity{Severity::INFO};                                     ///< Mức độ cảnh báo (DEBUG, INFO, WARN, ERROR, FATAL)

    SensorReading() = default;

    SensorReading(std::string id, double temp, double hum, Severity sev = Severity::INFO,
                  std::chrono::system_clock::time_point ts = std::chrono::system_clock::now())
        : sensorId(std::move(id)), temperature(temp), humidity(hum), timestamp(ts), severity(sev) {}

    // Constructor hỗ trợ epoch timestamp ms (tiện ích cho unit test & backward compatibility)
    SensorReading(int64_t epoch_ms, std::string id, double temp, double hum, Severity sev = Severity::INFO)
        : sensorId(std::move(id)),
          temperature(temp),
          humidity(hum),
          timestamp(std::chrono::system_clock::time_point(std::chrono::milliseconds(epoch_ms))),
          severity(sev) {}
};

/**
 * @brief Chuyển đổi timestamp sang chuỗi thời gian định dạng YYYY-MM-DD HH:MM:SS.mmm.
 */
inline std::string format_timestamp(const std::chrono::system_clock::time_point& tp) {
    auto now_c = std::chrono::system_clock::to_time_t(tp);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()) % 1000;
    std::tm tm_buf{};
#if defined(_WIN32) || defined(_WIN64)
    localtime_s(&tm_buf, &now_c);
#else
    localtime_r(&now_c, &tm_buf);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return oss.str();
}

/**
 * @brief Chuyển đổi SensorReading sang chuỗi định dạng: "S1|35.2|60.1|WARN|2025-...".
 */
inline std::string to_string(const SensorReading& r) {
    std::ostringstream oss;
    oss << r.sensorId << "|"
        << std::fixed << std::setprecision(1) << r.temperature << "|"
        << std::fixed << std::setprecision(1) << r.humidity << "|"
        << to_string(r.severity) << "|"
        << format_timestamp(r.timestamp);
    return oss.str();
}

/**
 * @brief Chuyển đổi SensorReading sang dòng CSV chuẩn: "timestamp,sensorId,temp,hum,sev".
 */
inline std::string toCsvLine(const SensorReading& r) {
    auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        r.timestamp.time_since_epoch())
                        .count();
    std::ostringstream oss;
    oss << epoch_ms << ","
        << r.sensorId << ","
        << std::fixed << std::setprecision(2) << r.temperature << ","
        << std::fixed << std::setprecision(2) << r.humidity << ","
        << to_string(r.severity);
    return oss.str();
}

/**
 * @brief Alias tương thích với toCsvLine.
 */
inline std::string to_csv_row(const SensorReading& r) {
    return toCsvLine(r);
}

/**
 * @brief Chuyển đổi SensorReading sang chuỗi định dạng JSON-like phục vụ WebSocket / Web UI.
 */
inline std::string to_json_str(const SensorReading& r) {
    auto epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        r.timestamp.time_since_epoch())
                        .count();
    std::ostringstream oss;
    oss << "{"
        << "\"timestamp\":" << epoch_ms << ","
        << "\"sensor_id\":\"" << r.sensorId << "\","
        << "\"temperature\":" << std::fixed << std::setprecision(2) << r.temperature << ","
        << "\"humidity\":" << std::fixed << std::setprecision(2) << r.humidity << ","
        << "\"severity\":\"" << to_string(r.severity) << "\""
        << "}";
    return oss.str();
}

/**
 * @brief Operator<< cho SensorReading để tương thích với std::cout.
 */
inline std::ostream& operator<<(std::ostream& os, const SensorReading& r) {
    os << to_string(r);
    return os;
}

} // namespace iot_stream
