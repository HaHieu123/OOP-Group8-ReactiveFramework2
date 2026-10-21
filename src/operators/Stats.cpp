#include "operators/Stats.h"
#include "core/Exceptions.h"

#include <algorithm>
#include <cmath>
#include <cstdint>

namespace iot_stream {

template <typename T>
T Stats<T>::sum(const Window<T>& window) {
    if (window.empty()) {
        return T{};
    }
    T total{};
    for (const auto& val : window) {
        total += val;
    }
    return total;
}

template <typename T>
double Stats<T>::avg(const Window<T>& window) {
    if (window.empty()) {
        return 0.0;
    }
    return static_cast<double>(sum(window)) / static_cast<double>(window.size());
}

template <typename T>
size_t Stats<T>::count(const Window<T>& window) {
    return window.size();
}

template <typename T>
std::pair<T, T> Stats<T>::minMax(const Window<T>& window) {
    if (window.empty()) {
        throw InvalidOperationException("Cannot compute minMax on an empty window.");
    }

    auto it = window.begin();
    T min_val = *it;
    T max_val = *it;
    ++it;

    for (; it != window.end(); ++it) {
        if (*it < min_val) min_val = *it;
        if (*it > max_val) max_val = *it;
    }

    return {min_val, max_val};
}

template <typename T>
double Stats<T>::variance(const Window<T>& window) {
    // Phương sai cho 0 hoặc 1 phần tử = 0.0
    if (window.empty() || window.size() <= 1) {
        return 0.0;
    }

    double mean = avg(window);
    double sum_squared_diff = 0.0;

    for (const auto& val : window) {
        double diff = static_cast<double>(val) - mean;
        sum_squared_diff += diff * diff;
    }

    // Phương sai tổng thể: sigma^2 = (1 / N) * sum((x_i - mean)^2)
    return sum_squared_diff / static_cast<double>(window.size());
}

template <typename T>
double Stats<T>::stddev(const Window<T>& window) {
    if (window.empty()) {
        return 0.0;
    }
    return std::sqrt(variance(window));
}

// ==============================================================================
// Explicit Template Instantiations
// Tránh lỗi undefined reference khi liên kết thư viện tĩnh iot_stream
// ==============================================================================

template class Stats<int>;
template class Stats<double>;
template class Stats<float>;
template class Stats<int64_t>;
template class Stats<uint64_t>;
template class Stats<int32_t>;
template class Stats<uint32_t>;

} // namespace iot_stream

