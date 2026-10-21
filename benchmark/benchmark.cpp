/**
 * @file benchmark.cpp
 * @brief Chương trình đo lường hiệu năng (Benchmark) cho IoT Stream Processing Framework.
 *
 * Nội dung kiểm thử:
 *   1. Throughput Test: Phát 1,000,000 phần tử qua chuỗi source → map → filter → sink,
 *      đo lường tốc độ xử lý sự kiện (events/giây).
 *   2. Latency Test: Đo thời gian truyền từ lúc emit đến khi sink nhận (p50, p95, p99).
 *   3. Backpressure Comparison: So sánh 4 chiến lược (BUFFER, DROP, LATEST, ERROR)
 *      khi Producer nhanh gấp 10 lần Consumer, in bảng so sánh chi tiết.
 *
 * Ràng buộc kỹ thuật:
 *   - Chuẩn C++17, chỉ dùng thư viện chuẩn STL và framework nội bộ.
 *   - Biên dịch sạch với MSVC 2022 (/std:c++17 /W4).
 */

#include "core/DataStream.h"
#include "core/Exceptions.h"
#include "source/SensorSource.h"
#include "operators/Operators.h"
#include "operators/Window.h"
#include "operators/Stats.h"
#include "backpressure/Backpressure.h"
#include "sinks/ConsoleSink.h"
#include "sinks/MemoryQueueSink.h"

#include "core/SimplePublisher.h"
#include "core/LambdaSubscriber.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#ifdef ERROR
#undef ERROR
#endif
#endif

using namespace iot_stream;
using Clock = std::chrono::high_resolution_clock;

// ─────────────────────────────────────────────────────────────────────────────
// Cấu trúc dữ liệu đo độ trễ cho từng sự kiện
// ─────────────────────────────────────────────────────────────────────────────
struct TimedEvent {
    int id{0};
    Clock::time_point t_emit;
};

// ─────────────────────────────────────────────────────────────────────────────
// Helper tính phân vị (Percentile) từ danh sách độ trễ đã sắp xếp
// ─────────────────────────────────────────────────────────────────────────────
static double compute_percentile(const std::vector<double>& sorted_data, double p) {
    if (sorted_data.empty()) return 0.0;
    size_t idx = static_cast<size_t>(p * static_cast<double>(sorted_data.size() - 1));
    if (idx >= sorted_data.size()) {
        idx = sorted_data.size() - 1;
    }
    return sorted_data[idx];
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 1: Đo thông lượng (Throughput Benchmark)
// Pipeline: Source -> Map (* 2) -> Filter (x % 2 == 0) -> LambdaSubscriber
// Số lượng: 1,000,000 phần tử
// ─────────────────────────────────────────────────────────────────────────────
void run_throughput_benchmark() {
    std::cout << "\n======================================================================\n";
    std::cout << " [TEST 1] Throughput Benchmark (1,000,000 Events qua Pipeline)\n";
    std::cout << " Pipeline: Source -> Map(*2) -> Filter(even) -> Sink\n";
    std::cout << "======================================================================\n";

    constexpr int TOTAL_EVENTS = 1'000'000;
    auto source = std::make_shared<SimplePublisher<int>>();
    std::atomic<uint64_t> received_count{0};

    auto sub = std::make_shared<LambdaSubscriber<int>>(
        [&](int /*val*/) {
            received_count.fetch_add(1, std::memory_order_relaxed);
        },
        [](const std::exception&) {},
        []() {}
    );

    auto stream = DataStream<int>(source)
        .map([](int x) { return x * 2; })
        .filter([](int x) { return x % 2 == 0; });
    stream.subscribe(sub);

    auto t_start = Clock::now();

    for (int i = 0; i < TOTAL_EVENTS; ++i) {
        source->emit(i);
    }
    source->complete();

    auto t_end = Clock::now();
    double duration_sec = std::chrono::duration<double>(t_end - t_start).count();
    double duration_ms = duration_sec * 1000.0;
    double events_per_sec = static_cast<double>(TOTAL_EVENTS) / duration_sec;

    std::cout << "  - Số phần tử phát đi : " << TOTAL_EVENTS << " events\n";
    std::cout << "  - Số phần tử nhận được: " << received_count.load() << " events\n";
    std::cout << "  - Thời gian xử lý     : " << std::fixed << std::setprecision(2)
              << duration_ms << " ms (" << duration_sec << " s)\n";
    std::cout << "  - THÔNG LƯỢNG (THROUGHPUT): " << std::fixed << std::setprecision(0)
              << events_per_sec << " events/giây\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 2: Đo độ trễ xử lý (Latency Benchmark)
// Đo thời gian từ lúc emit -> lúc subscriber nhận được cho 100,000 sự kiện.
// Tính các mốc phân vị: p50 (Median), p95, p99.
// ─────────────────────────────────────────────────────────────────────────────
void run_latency_benchmark() {
    std::cout << "\n======================================================================\n";
    std::cout << " [TEST 2] End-to-End Latency Benchmark (100,000 Events)\n";
    std::cout << " Phân vị độ trễ (Latency Percentiles): p50, p95, p99\n";
    std::cout << "======================================================================\n";

    constexpr int LATENCY_SAMPLES = 100'000;
    std::vector<double> latencies_us;
    latencies_us.reserve(LATENCY_SAMPLES);

    auto source = std::make_shared<SimplePublisher<TimedEvent>>();

    auto sub = std::make_shared<LambdaSubscriber<TimedEvent>>(
        [&](const TimedEvent& event) {
            auto t_recv = Clock::now();
            double latency_us = std::chrono::duration<double, std::micro>(t_recv - event.t_emit).count();
            latencies_us.push_back(latency_us);
        },
        [](const std::exception&) {},
        []() {}
    );

    auto stream = DataStream<TimedEvent>(source);
    stream.subscribe(sub);

    for (int i = 0; i < LATENCY_SAMPLES; ++i) {
        TimedEvent ev;
        ev.id = i;
        ev.t_emit = Clock::now();
        source->emit(ev);
    }
    source->complete();

    // Sắp xếp vector độ trễ để tính phân vị
    std::sort(latencies_us.begin(), latencies_us.end());

    double p50 = compute_percentile(latencies_us, 0.50);
    double p95 = compute_percentile(latencies_us, 0.95);
    double p99 = compute_percentile(latencies_us, 0.99);
    double min_lat = latencies_us.front();
    double max_lat = latencies_us.back();

    std::cout << "  - Số mẫu đo lường : " << latencies_us.size() << "\n";
    std::cout << "  - Min Latency     : " << std::fixed << std::setprecision(3) << min_lat << " µs\n";
    std::cout << "  - p50 (Median)    : " << std::fixed << std::setprecision(3) << p50     << " µs\n";
    std::cout << "  - p95             : " << std::fixed << std::setprecision(3) << p95     << " µs\n";
    std::cout << "  - p99             : " << std::fixed << std::setprecision(3) << p99     << " µs\n";
    std::cout << "  - Max Latency     : " << std::fixed << std::setprecision(3) << max_lat << " µs\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 3: So sánh 4 chiến lược Backpressure (BUFFER, DROP, LATEST, ERROR)
// Mô hình: Producer nhanh gấp 10 lần Consumer (1 demand cho mỗi 10 emits)
// ─────────────────────────────────────────────────────────────────────────────
struct StrategyResult {
    std::string strategy_name;
    double throughput{0.0};
    double p50_us{0.0};
    double p99_us{0.0};
    size_t dropped{0};
};

static StrategyResult benchmark_strategy(BackpressureStrategy strat, const std::string& name) {
    constexpr int NUM_ROUNDS = 2'000;
    constexpr int RATIO = 10; // Producer emit 10 phần tử cho mỗi 1 phần tử consumer yêu cầu
    constexpr int TOTAL_ITEMS = NUM_ROUNDS * RATIO; // 20,000 items

    std::vector<double> latencies_us;
    latencies_us.reserve(TOTAL_ITEMS);
    std::atomic<bool> error_occurred{false};

    auto sub = std::make_shared<LambdaSubscriber<TimedEvent>>(
        [&](const TimedEvent& ev) {
            auto t_recv = Clock::now();
            double lat = std::chrono::duration<double, std::micro>(t_recv - ev.t_emit).count();
            latencies_us.push_back(lat);
        },
        [&](const std::exception&) {
            error_occurred.store(true);
        },
        []() {},
        false // auto_request_max = false để kiểm soát demand thủ công
    );

    // BUFFER capacity = 200, DROP capacity = 200, LATEST capacity = 1, ERROR capacity = 0
    size_t capacity = (strat == BackpressureStrategy::ERROR) ? 0 :
                      (strat == BackpressureStrategy::LATEST) ? 1 : 200;

    auto bp_sub = makeBackpressureSubscription<TimedEvent>(strat, capacity, sub.get());
    sub->onSubscribe(bp_sub.get());

    auto t_start = Clock::now();

    for (int r = 0; r < NUM_ROUNDS; ++r) {
        if (error_occurred.load()) break;

        // Consumer cấp demand = 1
        bp_sub->request(1);

        // Producer phát 10 phần tử (nhanh gấp 10 lần)
        for (int k = 0; k < RATIO; ++k) {
            if (error_occurred.load()) break;
            TimedEvent ev;
            ev.id = r * RATIO + k;
            ev.t_emit = Clock::now();
            bp_sub->onNext(ev);
        }
    }

    auto t_end = Clock::now();
    double duration_sec = std::chrono::duration<double>(t_end - t_start).count();
    if (duration_sec <= 0.0) duration_sec = 1e-6;

    size_t processed = latencies_us.size();
    double throughput = static_cast<double>(processed) / duration_sec;

    double p50 = 0.0;
    double p99 = 0.0;
    if (!latencies_us.empty()) {
        std::sort(latencies_us.begin(), latencies_us.end());
        p50 = compute_percentile(latencies_us, 0.50);
        p99 = compute_percentile(latencies_us, 0.99);
    }

    size_t dropped = bp_sub->droppedCount();
    if (strat == BackpressureStrategy::ERROR && error_occurred.load()) {
        // ERROR strategy dừng ngay khi quá tải
        dropped = TOTAL_ITEMS - processed;
    }

    StrategyResult res;
    res.strategy_name = name;
    res.throughput = throughput;
    res.p50_us = p50;
    res.p99_us = p99;
    res.dropped = dropped;
    return res;
}

void run_backpressure_benchmark() {
    std::cout << "\n======================================================================\n";
    std::cout << " [TEST 3] Backpressure Strategy Comparison (Producer 10x Consumer)\n";
    std::cout << "======================================================================\n";

    std::vector<StrategyResult> results;
    results.push_back(benchmark_strategy(BackpressureStrategy::BUFFER, "BUFFER"));
    results.push_back(benchmark_strategy(BackpressureStrategy::DROP,   "DROP"));
    results.push_back(benchmark_strategy(BackpressureStrategy::LATEST, "LATEST"));
    results.push_back(benchmark_strategy(BackpressureStrategy::ERROR,  "ERROR"));

    // In bảng so sánh đẹp mắt dùng std::setw
    std::cout << "\n";
    std::cout << "+------------+-------------------+------------+------------+------------+\n";
    std::cout << "| Strategy   | Throughput (ev/s) | p50 (us)   | p99 (us)   | Dropped    |\n";
    std::cout << "+------------+-------------------+------------+------------+------------+\n";

    for (const auto& r : results) {
        std::cout << "| " << std::left  << std::setw(10) << r.strategy_name
                  << " | " << std::right << std::setw(17) << std::fixed << std::setprecision(0) << r.throughput
                  << " | " << std::right << std::setw(10) << std::fixed << std::setprecision(3) << r.p50_us
                  << " | " << std::right << std::setw(10) << std::fixed << std::setprecision(3) << r.p99_us
                  << " | " << std::right << std::setw(10) << r.dropped
                  << " |\n";
    }
    std::cout << "+------------+-------------------+------------+------------+------------+\n\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────
int main() {
#ifdef _WIN32
    // Cài đặt console hỗ trợ UTF-8 hiển thị tiếng Việt trên Windows
    SetConsoleOutputCP(CP_UTF8);
#endif

    std::cout << "======================================================================\n";
    std::cout << "   IoT Stream Framework — Performance & Benchmark Suite\n";
    std::cout << "======================================================================\n";

    try {
        run_throughput_benchmark();
        run_latency_benchmark();
        run_backpressure_benchmark();
    } catch (const std::exception& ex) {
        std::cerr << "[BENCHMARK ERROR] " << ex.what() << "\n";
        return 1;
    }

    std::cout << ">>> BENCHMARK HOÀN THÀNH THÀNH CÔNG <<<\n";
    return 0;
}
