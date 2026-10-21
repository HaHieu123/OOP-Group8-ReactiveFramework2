/**
 * @file test_stats.cpp
 * @brief Kiểm thử module Stats (đã merge vào test_window.cpp nhưng giữ file này
 *        như sanity check độc lập, không trùng lặp test).
 *
 * Test cases:
 *   Stats::IntWindow_Sum           — Window<int>: sum đúng
 *   Stats::IntWindow_AvgCount      — Window<int>: avg và count đúng
 *   Stats::FloatWindow_Variance    — Window<float>: variance/stddev gần đúng
 */

#include "test_framework.h"

#include "core/Exceptions.h"
#include "operators/Stats.h"
#include "operators/Window.h"

#include <cmath>

using namespace iot_stream;

// ─────────────────────────────────────────────────────────────────────────────
// Stats<int> — sum chính xác
// ─────────────────────────────────────────────────────────────────────────────
TEST(Stats, IntWindow_Sum) {
    Window<int> win;
    for (int i = 1; i <= 10; ++i) win.add(i); // 1+2+...+10 = 55

    ASSERT_EQ(Stats<int>::sum(win), 55);
    ASSERT_EQ(Stats<int>::count(win), static_cast<size_t>(10));
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats<int> — avg đúng, sử dụng window {1, 3, 5} → avg = 3.0
// ─────────────────────────────────────────────────────────────────────────────
TEST(Stats, IntWindow_AvgCount) {
    Window<int> win;
    win.add(1); win.add(3); win.add(5);

    ASSERT_NEAR(Stats<int>::avg(win), 3.0, 1e-9);
    ASSERT_EQ(Stats<int>::count(win), static_cast<size_t>(3));

    auto [mn, mx] = Stats<int>::minMax(win);
    ASSERT_EQ(mn, 1);
    ASSERT_EQ(mx, 5);
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats<float> — variance và stddev: dãy {1.0, 2.0, 3.0}
// mean = 2.0, variance = ((1-2)^2+(2-2)^2+(3-2)^2)/3 = 2/3 ≈ 0.6667
// ─────────────────────────────────────────────────────────────────────────────
TEST(Stats, FloatWindow_Variance) {
    Window<float> win;
    win.add(1.0f); win.add(2.0f); win.add(3.0f);

    double expected_var = 2.0 / 3.0;
    double expected_std = std::sqrt(expected_var);

    ASSERT_NEAR(Stats<float>::variance(win), expected_var, 1e-5);
    ASSERT_NEAR(Stats<float>::stddev(win),   expected_std, 1e-5);
}
