/**
 * @file test_operators.cpp
 * @brief Kiểm thử module Operators — Strategy Pattern (Map, Filter, FlatMap).
 *
 * Test cases:
 *   Operators::FilterOperation_PassThrough   — predicate đúng → có giá trị
 *   Operators::FilterOperation_NullOpt       — predicate sai → std::nullopt
 *   Operators::MapOperation_IntToString      — ánh xạ int → string
 *   Operators::MapOperation_DoubleToDouble   — ánh xạ double → double (tính toán)
 *   Operators::FlatMapOperation_Range        — phát nhiều phần tử từ một đầu vào
 *   Operators::FlatMapOperation_EmptyResult  — flatmap trả về vector rỗng
 */

#include "test_framework.h"

#include "operators/Operators.h"

#include <string>
#include <vector>

using namespace iot_stream;

// ─────────────────────────────────────────────────────────────────────────────
// FilterOperation — predicate đúng: trả giá trị gốc
// ─────────────────────────────────────────────────────────────────────────────
TEST(Operators, FilterOperation_PassThrough) {
    auto filter_even = makeFilter<int>([](int x) { return x % 2 == 0; });

    auto result_even = filter_even->apply(4);
    ASSERT_TRUE(result_even.has_value());
    ASSERT_EQ(result_even.value(), 4);

    auto result_zero = filter_even->apply(0);
    ASSERT_TRUE(result_zero.has_value());
    ASSERT_EQ(result_zero.value(), 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// FilterOperation — predicate sai: trả nullopt (giả lập filter loại bỏ)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Operators, FilterOperation_NullOpt) {
    auto filter_even = makeFilter<int>([](int x) { return x % 2 == 0; });

    auto result_odd = filter_even->apply(3);
    ASSERT_FALSE(result_odd.has_value());

    auto result_neg = filter_even->apply(-1);
    ASSERT_FALSE(result_neg.has_value());
}

// ─────────────────────────────────────────────────────────────────────────────
// MapOperation — ánh xạ int → string
// ─────────────────────────────────────────────────────────────────────────────
TEST(Operators, MapOperation_IntToString) {
    auto map_to_str = makeMap<int, std::string>([](int x) {
        return "NUM_" + std::to_string(x);
    });

    auto res_100 = map_to_str->apply(100);
    ASSERT_TRUE(res_100.has_value());
    ASSERT_EQ(res_100.value(), std::string("NUM_100"));

    auto res_neg = map_to_str->apply(-5);
    ASSERT_TRUE(res_neg.has_value());
    ASSERT_EQ(res_neg.value(), std::string("NUM_-5"));
}

// ─────────────────────────────────────────────────────────────────────────────
// MapOperation — ánh xạ double → double (nhân đôi giá trị)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Operators, MapOperation_DoubleToDouble) {
    auto double_val = makeMap<double, double>([](double x) { return x * 2.0; });

    auto r1 = double_val->apply(3.5);
    ASSERT_TRUE(r1.has_value());
    ASSERT_NEAR(r1.value(), 7.0, 1e-9);

    auto r2 = double_val->apply(0.0);
    ASSERT_TRUE(r2.has_value());
    ASSERT_NEAR(r2.value(), 0.0, 1e-9);
}

// ─────────────────────────────────────────────────────────────────────────────
// FlatMapOperation — phát nhiều phần tử từ một đầu vào
// ─────────────────────────────────────────────────────────────────────────────
TEST(Operators, FlatMapOperation_Range) {
    auto flatmap_range = makeFlatMap<int, int>([](int x) {
        return std::vector<int>{x, x + 1, x + 2};
    });

    auto flat_res = flatmap_range->apply(10);
    ASSERT_TRUE(flat_res.has_value());
    ASSERT_EQ(flat_res.value().size(), static_cast<size_t>(3));
    ASSERT_EQ(flat_res.value()[0], 10);
    ASSERT_EQ(flat_res.value()[1], 11);
    ASSERT_EQ(flat_res.value()[2], 12);
}

// ─────────────────────────────────────────────────────────────────────────────
// FlatMapOperation — trả về vector rỗng: has_value() = true nhưng rỗng
// ─────────────────────────────────────────────────────────────────────────────
TEST(Operators, FlatMapOperation_EmptyResult) {
    // Filter qua FlatMap: chỉ mở rộng số chẵn, số lẻ → vector rỗng
    auto flatmap_filter = makeFlatMap<int, int>([](int x) -> std::vector<int> {
        if (x % 2 == 0) return {x};
        return {};
    });

    auto odd_res = flatmap_filter->apply(3);
    ASSERT_TRUE(odd_res.has_value()); // FlatMap luôn trả optional với vector
    ASSERT_TRUE(odd_res.value().empty());

    auto even_res = flatmap_filter->apply(4);
    ASSERT_TRUE(even_res.has_value());
    ASSERT_EQ(even_res.value().size(), static_cast<size_t>(1));
    ASSERT_EQ(even_res.value()[0], 4);
}
