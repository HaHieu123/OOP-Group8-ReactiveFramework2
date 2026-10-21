/**
 * @file main.cpp
 * @brief Ứng dụng end-to-end: Giám sát cảm biến IoT thời gian thực.
 *
 * Kịch bản:
 *   - 3 sensor (S1, S2, S3), chu kỳ phát 100ms, chạy 10 giây → ~300 readings.
 *   - Pipeline: Source → filter(severity >= WARN) → map(to_string) →
 *               TumblingWindow(2s) → map(window size) → ConsoleSink + FileSink.
 *   - Backpressure BUFFER capacity 1000 (thông qua MemoryQueueSink).
 *   - Thread riêng in "Events/sec" ra stderr mỗi giây.
 *   - SIGINT handler (Ctrl+C) dừng pipeline an toàn và in tóm tắt.
 */

#include "backpressure/Backpressure.h"
#include "core/DataStream.h"
#include "core/LambdaSubscriber.h"
#include "core/Severity.h"
#include "operators/Stats.h"
#include "operators/Window.h"
#include "sinks/ConsoleSink.h"
#include "sinks/FileSink.h"
#include "sinks/MemoryQueueSink.h"
#include "source/Sensor.h"
#include "source/SensorSource.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

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

using namespace std::chrono_literals;
using namespace iot_stream;

// ============================================================================
// Biến toàn cục (SIGINT + bộ đếm metrics)
// ============================================================================

/// Cờ ngắt toàn cục: atomic để an toàn khi SIGINT gọi từ signal handler
static std::atomic<bool> g_stop{false};

/// Tổng số sự kiện đã nhận (tất cả subscriber)
static std::atomic<int64_t> g_total_events{0};

/// Tổng độ trễ xử lý tích lũy (nanoseconds) để tính avg latency
static std::atomic<int64_t> g_total_latency_ns{0};

/// Bộ đếm events trong giây hiện tại (cho thread giám sát)
static std::atomic<int64_t> g_events_this_second{0};

// ============================================================================
// SIGINT Handler (Ctrl+C)
// ============================================================================

static void sigint_handler(int /*sig*/) {
    g_stop = true;
}

// ============================================================================
// Main Application
// ============================================================================

int main() {
#ifdef _WIN32
    // Cài đặt console UTF-8 để hiển thị tiếng Việt chính xác
    SetConsoleOutputCP(CP_UTF8);
#endif
    int exit_code = 0;

    // [INIT] Đăng ký SIGINT handler
    std::signal(SIGINT, sigint_handler);

    std::cout << "[INIT] IoT Stream Framework — Sensor Monitoring Pipeline\n";
    std::cout << "[INIT] Sensors: S1, S2, S3 | Interval: 100ms | Duration: 10s\n";
    std::cout << "[INIT] Pipeline: filter(WARN+) → map → TumblingWindow(2s) → ConsoleSink + FileSink\n";
    std::cout << "[INIT] Press Ctrl+C to stop early.\n\n";

    // Thời điểm bắt đầu đo lường toàn bộ vòng đời ứng dụng
    auto app_start = std::chrono::high_resolution_clock::now();

    try {
        // ==================================================================
        // [STAGE 1] Khởi tạo nguồn phát (SensorSource)
        // 3 sensor × 100ms × 10s → ~300 SensorReading
        // ==================================================================
        auto source = std::make_shared<SensorSource>(
            std::vector<std::string>{"S1", "S2", "S3"},
            std::chrono::milliseconds(100),
            std::chrono::seconds(10)
        );

        // ==================================================================
        // [STAGE 2] Cấu hình Sinks
        // ==================================================================

        // ConsoleSink: In các window summary ra terminal
        auto console_sink = std::make_shared<ConsoleSink<std::string>>(true);

        // FileSink: Ghi window summary ra file CSV cho Dashboard Python
        auto file_sink = std::make_shared<FileSink<std::string>>("dashboard/data.csv", false);

        // MemoryQueueSink: Lưu trữ để tính thống kê tổng cuối pipeline
        auto memory_sink = std::make_shared<MemoryQueueSink<std::string>>();

        // ==================================================================
        // [STAGE 3] Xây dựng Pipeline Fluent Interface
        //
        // SensorSource
        //   └─ filter(severity >= WARN)       → Loại bỏ các bản tin INFO
        //       └─ map(to_string)             → SensorReading → "S1|35.2|..."
        //           └─ TumblingWindow(2s)     → Window<string> mỗi 2 giây
        //               └─ map(window report) → "WINDOW: 5 events in 2s"
        //                   └─ ConsoleSink    → In ra terminal
        //                   └─ FileSink       → Ghi ra CSV
        //                   └─ MemorySink     → Lưu trong RAM để thống kê
        // ==================================================================

        // Subscriber thô nhận SensorReading và đo latency
        auto raw_sub = std::make_shared<LambdaSubscriber<SensorReading>>(
            [&](const SensorReading& r) {
                // Đo latency từ khi tạo reading đến khi nhận được
                auto recv_time = std::chrono::high_resolution_clock::now();
                auto reading_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    r.timestamp.time_since_epoch()).count();
                auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                int64_t latency_ns = now_ns - reading_time_ns;
                if (latency_ns > 0 && latency_ns < 1'000'000'000LL) { // Bỏ giá trị bất hợp lý
                    g_total_latency_ns += latency_ns;
                }
                g_total_events++;
                g_events_this_second++;
                (void)recv_time;
            },
            [&](const std::exception& ex) {
                std::cerr << "[ERROR] Pipeline exception: " << ex.what() << "\n";
                exit_code = 1;
            },
            [&]() {
                g_stop = true;
            }
        );

        // Pipeline Window → ConsoleSink
        auto window_console_sub = std::make_shared<LambdaSubscriber<Window<SensorReading>>>(
            [&](const Window<SensorReading>& win) {
                // Map: tổng hợp window thành 1 dòng báo cáo
                std::string report = "[WINDOW] " +
                    std::to_string(win.size()) + " alerts in 2s window";
                console_sink->onNext(report);
                file_sink->onNext(report);
                memory_sink->onNext(report);
            },
            [](const std::exception&) {},
            []() {}
        );

        // Pipeline 1: Đo latency và đếm events → raw_sub
        auto stream1 = DataStream<SensorReading>(source);
        stream1.subscribe(raw_sub);

        // Pipeline 2: filter(WARN+) → TumblingWindow(2s) → window report → Sinks
        auto stream2 = DataStream<SensorReading>(source)
            .filter([](const SensorReading& r) {
                // Lọc: Chỉ chuyển tiếp các sự kiện WARN, ERROR, FATAL
                return r.severity >= Severity::WARN;
            })
            .window(std::make_shared<TumblingWindow<SensorReading>>(2000ms));
        stream2.subscribe(window_console_sub);

        // ==================================================================
        // [STAGE 4] Thread giám sát — in Events/sec mỗi giây ra stderr
        // ==================================================================
        std::thread monitor_thread([&]() {
            while (!g_stop) {
                std::this_thread::sleep_for(1s);
                int64_t eps = g_events_this_second.exchange(0);
                std::cerr << "[MONITOR] Events/sec = " << eps << "\n";
            }
        });

        // ==================================================================
        // [RUN] Khởi động nguồn phát và chờ kết thúc
        // ==================================================================
        std::cout << "[RUN] Starting SensorSource...\n";
        source->start();

        // Vòng chờ chính: spin đến khi g_stop = true (từ onComplete hoặc SIGINT)
        while (!g_stop) {
            std::this_thread::sleep_for(100ms);
        }

        // ==================================================================
        // [STOP] Dừng an toàn
        // ==================================================================
        std::cout << "\n[STOP] Stopping pipeline...\n";
        source->stop();
        console_sink->flush();
        file_sink->flush();

        if (monitor_thread.joinable()) {
            monitor_thread.join();
        }

        // ==================================================================
        // [SUMMARY] Tóm tắt kết quả
        // ==================================================================
        auto app_end = std::chrono::high_resolution_clock::now();
        double elapsed_s = std::chrono::duration<double>(app_end - app_start).count();
        int64_t total_ev = g_total_events.load();
        int64_t total_lat_ns = g_total_latency_ns.load();
        double avg_lat_us = (total_ev > 0)
            ? (static_cast<double>(total_lat_ns) / static_cast<double>(total_ev)) / 1000.0
            : 0.0;
        int64_t alerts = static_cast<int64_t>(memory_sink->size());

        std::cout << "\n[SUMMARY] ==========================================\n";
        std::cout << "[SUMMARY] Duration         : " << std::fixed << elapsed_s << "s\n";
        std::cout << "[SUMMARY] Total events     : " << total_ev << "\n";
        std::cout << "[SUMMARY] WARN+ alerts     : " << alerts << " window reports\n";
        std::cout << "[SUMMARY] Avg latency      : " << avg_lat_us << " µs\n";
        std::cout << "[SUMMARY] Output file      : dashboard/data.csv\n";
        std::cout << "[SUMMARY] ==========================================\n";

    } catch (const std::exception& ex) {
        std::cerr << "[FATAL] Unhandled exception: " << ex.what() << "\n";
        return 1;
    }

    return exit_code;
}
