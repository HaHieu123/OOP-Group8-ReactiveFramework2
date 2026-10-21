/**
 * @file test_window.cpp
 * @brief Kiểm thử toàn diện module Windowing và module Stats.
 *
 * Danh sách test cases (không trùng lặp):
 *   1. TEST(Window_Tumbling_Basic)         — TumblingWindow: thêm 5 elements, window 2s, emit đúng.
 *   2. TEST(Window_Sliding_Basic)          — SlidingWindow: kiểm tra cửa sổ trượt phát sự kiện.
 *   3. TEST(Window_Count_Basic)            — CountWindow: 100 elements, window 10, phát đúng 10 lần.
 *   4. TEST(Window_Stats_Sum)              — Stats<int>::sum([1,2,3,4,5]) == 15.
 *   5. TEST(Window_Stats_Avg)              — Stats<int>::avg([1,2,3,4,5]) == 3.0.
 *   6. TEST(Window_Stats_Count)            — Stats<int>::count([1,2,3,4,5]) == 5.
 *   7. TEST(Window_Stats_MinMax)           — Stats<int>::minMax([1,2,3,4,5]) == (1, 5).
 *   8. TEST(Window_Stats_EmptyWindow)      — Window rỗng: sum=0, avg=0.0, count=0 (không ném lỗi).
 *   9. TEST(Window_Stats_MinMaxThrowsOnEmpty) — Window rỗng: minMax ném InvalidOperationException.
 *  10. TEST(Window_Stats_VarianceStddev)   — Tính variance/stddev chính xác cho kiểu double.
 */

#include "test_framework.h"

#include "core/DataStream.h"
#include "core/Exceptions.h"
#include "core/LambdaSubscriber.h"
#include "core/SimplePublisher.h"
#include "operators/Stats.h"
#include "operators/Window.h"

#include <chrono>
#include <cmath>
#include <thread>
#include <vector>

using namespace iot_stream;
using namespace std::chrono_literals;

// ─────────────────────────────────────────────────────────────────────────────
// 1. TumblingWindow: thêm 5 elements, window 2s, complete flush đúng 5 phần tử
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Tumbling_Basic) {
    auto source = std::make_shared<SimplePublisher<int>>();
    std::vector<Window<int>> received_windows;
    bool completed = false;

    auto sub = std::make_shared<LambdaSubscriber<Window<int>>>(
        [&](const Window<int>& win) {
            received_windows.push_back(win);
        },
        [](const std::exception&) {},
        [&]() { completed = true; }
    );

    auto stream = DataStream<int>(source).window(std::make_shared<TumblingWindow<int>>(2000ms));
    stream.subscribe(sub);

    // Phát 5 phần tử
    for (int i = 1; i <= 5; ++i) {
        source->emit(i);
    }
    source->complete(); // complete flush nốt cửa sổ hiện tại

    ASSERT_EQ(received_windows.size(), static_cast<size_t>(1));
    ASSERT_EQ(received_windows[0].size(), static_cast<size_t>(5));
    ASSERT_EQ(received_windows[0].elements()[0], 1);
    ASSERT_EQ(received_windows[0].elements()[4], 5);
    ASSERT_TRUE(completed);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. SlidingWindow: cửa sổ trượt, kiểm tra phát sự kiện khi trượt
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Sliding_Basic) {
    auto source = std::make_shared<SimplePublisher<int>>();
    std::vector<Window<int>> received_windows;

    auto sub = std::make_shared<LambdaSubscriber<Window<int>>>(
        [&](const Window<int>& win) {
            received_windows.push_back(win);
        }
    );

    // Cửa sổ 100ms, trượt mỗi 40ms
    auto stream = DataStream<int>(source).window(
        std::make_shared<SlidingWindow<int>>(100ms, 40ms));
    stream.subscribe(sub);

    source->emit(10);
    std::this_thread::sleep_for(50ms); // > 40ms slide interval
    source->emit(20);
    source->complete();

    ASSERT_TRUE(!received_windows.empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. CountWindow: 100 elements, window 10, phát đúng 10 lần, mỗi lần 10 phần tử
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Count_Basic) {
    auto source = std::make_shared<SimplePublisher<int>>();
    std::vector<size_t> window_sizes;
    bool completed = false;

    auto sub = std::make_shared<LambdaSubscriber<Window<int>>>(
        [&](const Window<int>& win) {
            window_sizes.push_back(win.size());
        },
        [](const std::exception&) {},
        [&]() { completed = true; }
    );

    auto stream = DataStream<int>(source).window(std::make_shared<CountWindow<int>>(10));
    stream.subscribe(sub);

    // Phát đúng 100 phần tử (từ 0 đến 99)
    for (int i = 0; i < 100; ++i) {
        source->emit(i);
    }
    source->complete();

    // 100 phần tử / 10 phần tử mỗi window = đúng 10 windows
    ASSERT_EQ(window_sizes.size(), static_cast<size_t>(10));
    for (size_t sz : window_sizes) {
        ASSERT_EQ(sz, static_cast<size_t>(10));
    }
    ASSERT_TRUE(completed);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Stats: Tính tổng các phần tử trong Window (1+2+3+4+5 = 15)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Stats_Sum) {
    Window<int> win;
    for (int i = 1; i <= 5; ++i) {
        win.add(i);
    }
    ASSERT_EQ(Stats<int>::sum(win), 15);
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Stats: Tính giá trị trung bình cộng (15 / 5 = 3.0)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Stats_Avg) {
    Window<int> win;
    for (int i = 1; i <= 5; ++i) {
        win.add(i);
    }
    ASSERT_NEAR(Stats<int>::avg(win), 3.0, 1e-9);
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Stats: Đếm số lượng phần tử trong Window (count == 5)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Stats_Count) {
    Window<int> win;
    for (int i = 1; i <= 5; ++i) {
        win.add(i);
    }
    ASSERT_EQ(Stats<int>::count(win), static_cast<size_t>(5));
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Stats: Tìm giá trị nhỏ nhất và lớn nhất (min=1, max=5)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Stats_MinMax) {
    Window<int> win;
    for (int i = 1; i <= 5; ++i) {
        win.add(i);
    }
    auto [min_val, max_val] = Stats<int>::minMax(win);
    ASSERT_EQ(min_val, 1);
    ASSERT_EQ(max_val, 5);
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Stats: Xử lý Window rỗng không ném ngoại lệ (sum=0, avg=0.0, count=0)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Stats_EmptyWindow) {
    Window<int> win;
    ASSERT_EQ(Stats<int>::count(win), static_cast<size_t>(0));
    ASSERT_EQ(Stats<int>::sum(win), 0);
    ASSERT_NEAR(Stats<int>::avg(win), 0.0, 1e-9);
    ASSERT_NEAR(Stats<int>::variance(win), 0.0, 1e-9);
    ASSERT_NEAR(Stats<int>::stddev(win), 0.0, 1e-9);
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. Stats: minMax trên Window rỗng phải ném InvalidOperationException
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Stats_MinMaxThrowsOnEmpty) {
    Window<int> win;
    ASSERT_THROW(Stats<int>::minMax(win), InvalidOperationException);
}

// ─────────────────────────────────────────────────────────────────────────────
// 10. Stats: Tính phương sai (variance) và độ lệch chuẩn (stddev) cho double
// Dãy: {2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0} -> N=8, Mean=5.0
// sum((x-mean)^2) = 9 + 1 + 1 + 1 + 0 + 0 + 4 + 16 = 32.0
// Variance = 32.0 / 8 = 4.0, StdDev = sqrt(4.0) = 2.0
// ─────────────────────────────────────────────────────────────────────────────
TEST(Window_Stats_VarianceStddev) {
    Window<double> win;
    win.add(2.0); win.add(4.0); win.add(4.0); win.add(4.0);
    win.add(5.0); win.add(5.0); win.add(7.0); win.add(9.0);

    ASSERT_NEAR(Stats<double>::variance(win), 4.0, 1e-9);
    ASSERT_NEAR(Stats<double>::stddev(win), 2.0, 1e-9);
}
