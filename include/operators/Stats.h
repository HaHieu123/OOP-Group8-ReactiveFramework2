#pragma once

#include "core/Exceptions.h"
#include "operators/Window.h"

#include <cstddef>
#include <type_traits>
#include <utility>

namespace iot_stream {

/**
 * @brief Lớp tiện ích cung cấp các hàm thống kê và tổng hợp (Aggregations) trên Window<T>.
 * 
 * Chỉ hỗ trợ các kiểu dữ liệu số học (Arithmetic types: int, float, double, ...).
 * 
 * Các phép toán thống kê hỗ trợ:
 * - sum: Tổng các giá trị trong window.
 * - avg: Giá trị trung bình (Mean / Average).
 * - count: Số lượng phần tử trong window.
 * - minMax: Giá trị nhỏ nhất và lớn nhất {min, max}.
 * - variance: Phương sai (độ phân tán của dữ liệu xung quanh giá trị trung bình).
 * - stddev: Độ lệch chuẩn (căn bậc hai của phương sai).
 * 
 * @tparam T Kiểu dữ liệu số học của các phần tử trong Window.
 */
template <typename T>
class Stats {
    static_assert(std::is_arithmetic_v<T>, "Stats only supports arithmetic types (int, float, double, etc.)");

public:
    /**
     * @brief Tính tổng các phần tử trong Window.
     * @return T Tổng các phần tử. Nếu Window rỗng, trả về T{} (0).
     */
    static T sum(const Window<T>& window);

    /**
     * @brief Tính giá trị trung bình cộng (Mean / Average).
     * @return double Giá trị trung bình. Nếu Window rỗng, trả về 0.0.
     */
    static double avg(const Window<T>& window);

    /**
     * @brief Đếm số lượng phần tử trong Window.
     * @return size_t Số lượng phần tử (trả về 0 nếu rỗng).
     */
    static size_t count(const Window<T>& window);

    /**
     * @brief Tìm giá trị nhỏ nhất và lớn nhất {min, max} trong Window.
     * @return std::pair<T, T> Cặp giá trị {min, max}.
     * @throws InvalidOperationException nếu Window rỗng.
     */
    static std::pair<T, T> minMax(const Window<T>& window);

    /**
     * @brief Tính phương sai tổng thể (Population Variance: sigma^2).
     * 
     * Công thức phương sai (Variance):
     *   sigma^2 = (1 / N) * sum((x_i - mean)^2)
     * 
     * Giải thích:
     * - Đo lường mức độ dao động hoặc phân tán của các giá trị cảm biến so với giá trị trung bình.
     * - Nếu tất cả các giá trị đo đạc bằng nhau, phương sai = 0.
     * - Nếu Window rỗng hoặc chỉ có 1 phần tử, trả về 0.0.
     * 
     * @return double Giá trị phương sai.
     */
    static double variance(const Window<T>& window);

    /**
     * @brief Tính độ lệch chuẩn (Standard Deviation: sigma).
     * 
     * Công thức độ lệch chuẩn:
     *   sigma = sqrt(variance)
     * 
     * Giải thích:
     * - Cùng đơn vị đo lường với dữ liệu cảm biến gốc (ví dụ: độ C, bar, %).
     * - Thường được dùng để phát hiện bất thường qua quy tắc 3-Sigma (3-Sigma Rule).
     * 
     * @return double Giá trị độ lệch chuẩn. Nếu Window rỗng, trả về 0.0.
     */
    static double stddev(const Window<T>& window);
};

} // namespace iot_stream

/*
================================================================================
Ví dụ sử dụng Module Stats tổng hợp dữ liệu Window IoT:
================================================================================

#include "operators/Stats.h"
#include "operators/Window.h"
#include "core/DataStream.h"
#include <iostream>
#include <chrono>

using namespace std::chrono_literals;
using namespace iot_stream;

struct TemperatureTelemetry {
    double avg_temp;
    double min_temp;
    double max_temp;
    double std_dev;
};

void example_window_stats(std::shared_ptr<Publisher<double>> temp_sensor_source) {
    // Pipeline: Gom nhóm nhiệt độ mỗi 5 giây và tính toán các chỉ số thống kê
    auto stats_stream = DataStream<double>(temp_sensor_source)
        .window(std::make_shared<TumblingWindow<double>>(5000ms))
        .map([](const Window<double>& win) -> TemperatureTelemetry {
            // Trường hợp window có dữ liệu
            auto [min_t, max_t] = win.empty() ? std::make_pair(0.0, 0.0) : Stats<double>::minMax(win);

            return TemperatureTelemetry{
                Stats<double>::avg(win),
                min_t,
                max_t,
                Stats<double>::stddev(win)
            };
        });
}
================================================================================
*/

