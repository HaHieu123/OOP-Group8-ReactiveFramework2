#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace iot_stream {

/**
 * @brief Strategy Interface cơ sở cho tất cả các toán tử biến đổi luồng.
 * 
 * Áp dụng Strategy Pattern:
 * - Mỗi toán tử (Map, Filter, FlatMap, ...) là một Strategy độc lập, đóng gói một giải thuật
 *   xử lý dữ liệu cụ thể.
 * 
 * Vì sao sử dụng Strategy Pattern thay vì chỉ dùng Lambda thuần?
 * 1. Dễ Unit Test độc lập: Có thể mock, inject và kiểm thử từng toán tử chuyên biệt mà không cần
 *    phải dựng toàn bộ pipeline phức tạp.
 * 2. Khả năng kết hợp (Composability): Dễ dàng xâu chuỗi, lồng ghép hoặc tạo Composite Strategy
 *    kết hợp nhiều phép biến đổi lại với nhau.
 * 3. Giám sát & Đo lường (Metrics/Observability): Dễ dàng bọc thêm decorator đo lường độ trễ (latency),
 *    đếm số lượng bản tin (throughput counter), hoặc ghi audit log tại từng bước trong luồng.
 * 4. Tái sử dụng (Reusability): Một Strategy toán tử nghiệp vụ (ví dụ: CalibrateSensorData,
 *    CheckAnomalyThreshold) có thể được lưu trữ và dùng lại trong nhiều pipeline khác nhau.
 * 
 * @tparam In Kiểu dữ liệu đầu vào.
 * @tparam Out Kiểu dữ liệu đầu ra sau biến đổi.
 */
template <typename In, typename Out>
class IStreamOperation {
public:
    virtual ~IStreamOperation() = default;

    /**
     * @brief Áp dụng toán tử lên phần tử dữ liệu đầu vào.
     * @param in Phần tử dữ liệu đầu vào.
     * @return std::optional<Out> Kết quả sau biến đổi. Trả về std::nullopt nếu phần tử
     *         bị loại bỏ (ví dụ trong FilterOperation).
     */
    virtual std::optional<Out> apply(const In& in) = 0;
};

/**
 * @brief Strategy biến đổi dữ liệu 1-1 (Map): In -> Out.
 * @tparam In Kiểu dữ liệu đầu vào.
 * @tparam Out Kiểu dữ liệu đầu ra.
 */
template <typename In, typename Out>
class MapOperation : public IStreamOperation<In, Out> {
public:
    using MapperFn = std::function<Out(const In&)>;

    explicit MapOperation(MapperFn mapper) : mapper_(std::move(mapper)) {}

    std::optional<Out> apply(const In& in) override {
        if (!mapper_) return std::nullopt;
        return mapper_(in);
    }

private:
    MapperFn mapper_;
};

/**
 * @brief Strategy lọc dữ liệu (Filter): In -> In (hoặc loại bỏ).
 * @tparam T Kiểu dữ liệu của phần tử cần lọc.
 */
template <typename T>
class FilterOperation : public IStreamOperation<T, T> {
public:
    using PredicateFn = std::function<bool(const T&)>;

    explicit FilterOperation(PredicateFn predicate) : predicate_(std::move(predicate)) {}

    std::optional<T> apply(const T& in) override {
        if (!predicate_ || !predicate_(in)) {
            return std::nullopt;
        }
        return in;
    }

private:
    PredicateFn predicate_;
};

/**
 * @brief Strategy làm phẳng dữ liệu 1-N (FlatMap): In -> std::vector<Out>.
 * @tparam In Kiểu dữ liệu đầu vào.
 * @tparam Out Kiểu dữ liệu của từng phần tử trong tập kết quả.
 */
template <typename In, typename Out>
class FlatMapOperation : public IStreamOperation<In, std::vector<Out>> {
public:
    using FlattenerFn = std::function<std::vector<Out>(const In&)>;

    explicit FlatMapOperation(FlattenerFn flattener) : flattener_(std::move(flattener)) {}

    std::optional<std::vector<Out>> apply(const In& in) override {
        if (!flattener_) return std::nullopt;
        return flattener_(in);
    }

private:
    FlattenerFn flattener_;
};

// ==============================================================================
// Factory Helpers (Inline Factory Functions)
// ==============================================================================

/**
 * @brief Tạo MapOperation dạng std::shared_ptr.
 */
template <typename In, typename Out>
inline std::shared_ptr<MapOperation<In, Out>> makeMap(std::function<Out(const In&)> mapper) {
    return std::make_shared<MapOperation<In, Out>>(std::move(mapper));
}

template <typename In, typename Out, typename F>
inline std::shared_ptr<MapOperation<In, Out>> makeMap(F mapper) {
    return std::make_shared<MapOperation<In, Out>>(std::function<Out(const In&)>(std::move(mapper)));
}

/**
 * @brief Tạo FilterOperation dạng std::shared_ptr.
 */
template <typename T>
inline std::shared_ptr<FilterOperation<T>> makeFilter(std::function<bool(const T&)> predicate) {
    return std::make_shared<FilterOperation<T>>(std::move(predicate));
}

/**
 * @brief Tạo FlatMapOperation dạng std::shared_ptr.
 */
template <typename In, typename Out>
inline std::shared_ptr<FlatMapOperation<In, Out>> makeFlatMap(std::function<std::vector<Out>(const In&)> flattener) {
    return std::make_shared<FlatMapOperation<In, Out>>(std::move(flattener));
}

template <typename In, typename Out, typename F>
inline std::shared_ptr<FlatMapOperation<In, Out>> makeFlatMap(F flattener) {
    return std::make_shared<FlatMapOperation<In, Out>>(std::function<std::vector<Out>(const In&)>(std::move(flattener)));
}

} // namespace iot_stream

/*
================================================================================
Ví dụ sử dụng Module Operators (Strategy Pattern):
================================================================================

#include "operators/Operators.h"
#include <iostream>
#include <cassert>

void example_operators_strategy() {
    using namespace iot_stream;

    // 1. Tạo Filter Strategy: Lọc các số chẵn
    auto is_even = makeFilter<int>([](int x) { return x % 2 == 0; });

    // 2. Tạo Map Strategy: Chuyển đổi số nguyên thành chuỗi định dạng
    auto to_string_op = makeMap<int, std::string>([](int x) {
        return "Value: " + std::to_string(x);
    });

    // 3. Tạo FlatMap Strategy: Tách chuỗi thành danh sách các token
    auto duplicate_op = makeFlatMap<int, int>([](int x) {
        return std::vector<int>{x, x * 10};
    });

    // Kiểm thử độc lập từng toán tử:
    assert(is_even->apply(2).has_value());
    assert(!is_even->apply(3).has_value()); // Bị filter -> nullopt

    auto mapped = to_string_op->apply(42);
    assert(mapped.has_value() && mapped.value() == "Value: 42");

    auto flat = duplicate_op->apply(5);
    assert(flat.has_value() && flat.value().size() == 2);
    assert(flat.value()[0] == 5 && flat.value()[1] == 50);

    std::cout << "[SUCCESS] Operators Strategy pattern verified successfully!\n";
}
================================================================================
*/

