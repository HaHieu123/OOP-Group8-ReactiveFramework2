#pragma once

#include "core/Exceptions.h"
#include <string>

namespace iot_stream {

/**
 * @brief Mức độ nghiêm trọng của sự kiện/cảnh báo trong hệ thống IoT.
 * 
 * Thứ bậc: DEBUG (0) < INFO (1) < WARN (2) < ERROR (3) < FATAL (4).
 */
enum class Severity {
    DEBUG = 0,
    INFO = 1,
    WARN = 2,
    ERROR = 3,
    FATAL = 4
};

/**
 * @brief Chuyển đổi giá trị enum Severity sang chuỗi đại diện (std::string).
 * 
 * @param s Giá trị Severity.
 * @return std::string Tên mức độ tương ứng ("DEBUG", "INFO", "WARN", "ERROR", "FATAL").
 */
inline std::string to_string(Severity s) {
    switch (s) {
        case Severity::DEBUG: return "DEBUG";
        case Severity::INFO:  return "INFO";
        case Severity::WARN:  return "WARN";
        case Severity::ERROR: return "ERROR";
        case Severity::FATAL: return "FATAL";
        default:              return "UNKNOWN";
    }
}

/**
 * @brief Chuyển đổi từ chuỗi sang giá trị enum Severity tương ứng.
 * 
 * @param str Chuỗi mô tả mức độ (ví dụ "DEBUG", "INFO", "WARN", "ERROR", "FATAL").
 * @return Severity Giá trị enum Severity tương ứng.
 * @throws InvalidOperationException nếu chuỗi đầu vào không khớp với bất kỳ mức độ nào.
 */
inline Severity severity_from_string(const std::string& str) {
    if (str == "DEBUG" || str == "debug") return Severity::DEBUG;
    if (str == "INFO"  || str == "info")  return Severity::INFO;
    if (str == "WARN"  || str == "warn" || str == "WARNING" || str == "warning") return Severity::WARN;
    if (str == "ERROR" || str == "error") return Severity::ERROR;
    if (str == "FATAL" || str == "fatal") return Severity::FATAL;

    throw InvalidOperationException("Invalid severity string: '" + str +
                                    "'. Expected DEBUG, INFO, WARN, ERROR, or FATAL.");
}

/**
 * @brief Toán tử so sánh lớn hơn hoặc bằng (>=) giữa hai mức Severity.
 * 
 * Trong C++17, scoped enum (`enum class`) có kiểu nguyên ngầm định đã tự động
 * hỗ trợ các toán tử so sánh (==, !=, <, <=, >, >=).
 * Hàm này được định nghĩa tường minh để phục vụ việc lọc dữ liệu trong pipeline
 * (ví dụ: filter([](const auto& d) { return d.severity >= Severity::WARN; })).
 */
inline constexpr bool operator>=(Severity lhs, Severity rhs) noexcept {
    return static_cast<int>(lhs) >= static_cast<int>(rhs);
}

} // namespace iot_stream

