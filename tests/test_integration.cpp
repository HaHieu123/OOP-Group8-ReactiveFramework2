/**
 * @file test_integration.cpp
 * @brief Kiểm thử tích hợp end-to-end: SensorSource → pipeline → Sink.
 *
 * Kịch bản: Mô phỏng pipeline IoT đơn giản trong môi trường test (thời lượng ngắn).
 *
 * Test cases:
 *   Integration::SensorSource_EmitsReadings    — SensorSource phát readings trong 1s
 *   Integration::FilterWARN_ReducesCount       — filter severity>=WARN giảm số readings
 *   Integration::CountWindow_CollectsInSink    — CountWindow(5) gom đủ 5 → MemoryQueueSink
 *   Integration::SensorReading_ToString        — to_string() format đúng
 *   Integration::SensorReading_ToCsvLine       — toCsvLine() format CSV đúng
 *   Integration::Pipeline_MapToString          — map SensorReading → string → MemoryQueueSink
 */

#include "test_framework.h"

#include "core/DataStream.h"
#include "core/LambdaSubscriber.h"
#include "core/SimplePublisher.h"
#include "operators/Window.h"
#include "sinks/MemoryQueueSink.h"
#include "source/Sensor.h"
#include "source/SensorSource.h"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace iot_stream;
using namespace std::chrono_literals;

// ─────────────────────────────────────────────────────────────────────────────
// SensorSource — phát đủ readings trong khoảng thời gian ngắn (1 sensor, 1s, 100ms)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Integration, SensorSource_EmitsReadings) {
    std::atomic<int> count{0};
    bool completed = false;

    // 1 sensor, 100ms interval, 1s duration → khoảng 10 readings
    auto source = std::make_shared<SensorSource>(
        std::vector<std::string>{"S_TEST"},
        100ms,
        std::chrono::seconds(1)
    );

    auto sub = std::make_shared<LambdaSubscriber<SensorReading>>(
        [&](const SensorReading& r) {
            (void)r;
            ++count;
        },
        [](const std::exception&) {},
        [&]() { completed = true; }
    );

    source->subscribe(sub.get());
    source->start();

    // Chờ source kết thúc (tối đa 2s để tránh test bị treo)
    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!completed && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(50ms);
    }

    // Phải nhận được ít nhất 5 readings (cho phép margin thời gian)
    ASSERT_TRUE(count.load() >= 5);
    ASSERT_TRUE(completed);
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter pipeline — SimplePublisher + filter severity>=WARN → MemoryQueueSink
// Emit hỗn hợp INFO/WARN/ERROR → chỉ WARN và ERROR qua được
// ─────────────────────────────────────────────────────────────────────────────
TEST(Integration, FilterWARN_ReducesCount) {
    auto source = std::make_shared<SimplePublisher<SensorReading>>();
    auto sink   = makeMemoryQueueSink<SensorReading>();

    auto sub = std::make_shared<LambdaSubscriber<SensorReading>>(
        [&](const SensorReading& r) { sink->onNext(r); },
        [](const std::exception&) {},
        [&]() { sink->onComplete(); }
    );

    // Pipeline: filter severity >= WARN
    auto stream = DataStream<SensorReading>(source)
        .filter([](const SensorReading& r) {
            return r.severity >= Severity::WARN;
        });
    stream.subscribe(sub);

    // Phát 5 readings: 3 INFO (bị loại), 2 WARN/ERROR (giữ lại)
    source->emit(SensorReading{"S1", 20.0, 50.0, Severity::INFO});
    source->emit(SensorReading{"S1", 35.0, 60.0, Severity::WARN});  // giữ
    source->emit(SensorReading{"S2", 22.0, 48.0, Severity::INFO});
    source->emit(SensorReading{"S2", 40.0, 70.0, Severity::ERROR}); // giữ
    source->emit(SensorReading{"S3", 18.0, 45.0, Severity::INFO});
    source->complete();

    ASSERT_EQ(sink->size(), static_cast<size_t>(2));
    auto items = sink->getQueue();
    ASSERT_EQ(static_cast<int>(items[0].severity), static_cast<int>(Severity::WARN));
    ASSERT_EQ(static_cast<int>(items[1].severity), static_cast<int>(Severity::ERROR));
}

// ─────────────────────────────────────────────────────────────────────────────
// CountWindow pipeline — emit đủ N phần tử → MemoryQueueSink nhận cửa sổ
// ─────────────────────────────────────────────────────────────────────────────
TEST(Integration, CountWindow_CollectsInSink) {
    auto source      = std::make_shared<SimplePublisher<int>>();
    auto window_sink = makeMemoryQueueSink<Window<int>>();

    auto sub = std::make_shared<LambdaSubscriber<Window<int>>>(
        [&](const Window<int>& w) { window_sink->onNext(w); },
        [](const std::exception&) {},
        [&]() { window_sink->onComplete(); }
    );

    // Pipeline: CountWindow kích thước 5
    DataStream<int>(source)
        .window(std::make_shared<CountWindow<int>>(5))
        .subscribe(sub);

    // Emit 12 phần tử → 2 cửa sổ đầy (5+5) + 1 residual (2)
    for (int i = 1; i <= 12; ++i) source->emit(i);
    source->complete();

    ASSERT_EQ(window_sink->size(), static_cast<size_t>(3));
    auto windows = window_sink->getQueue();
    ASSERT_EQ(windows[0].size(), static_cast<size_t>(5));
    ASSERT_EQ(windows[1].size(), static_cast<size_t>(5));
    ASSERT_EQ(windows[2].size(), static_cast<size_t>(2));

    // Kiểm tra nội dung cửa sổ đầu tiên: {1, 2, 3, 4, 5}
    ASSERT_EQ(windows[0].elements()[0], 1);
    ASSERT_EQ(windows[0].elements()[4], 5);
}

// ─────────────────────────────────────────────────────────────────────────────
// SensorReading — to_string() chứa đủ các trường theo định dạng pipe-separated
// ─────────────────────────────────────────────────────────────────────────────
TEST(Integration, SensorReading_ToString) {
    SensorReading r{1700000000000LL, "S_UNIT", 32.5, 65.0, Severity::WARN};
    std::string str = to_string(r);

    ASSERT_TRUE(str.find("S_UNIT") != std::string::npos);
    ASSERT_TRUE(str.find("32.5")   != std::string::npos);
    ASSERT_TRUE(str.find("65")     != std::string::npos);
    ASSERT_TRUE(str.find("WARN")   != std::string::npos);
}

// ─────────────────────────────────────────────────────────────────────────────
// SensorReading — toCsvLine() format CSV đúng (comma-separated, có header info)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Integration, SensorReading_ToCsvLine) {
    SensorReading r{1700000000000LL, "S_CSV", 28.0, 55.0, Severity::INFO};
    std::string csv = toCsvLine(r);

    ASSERT_TRUE(csv.find("S_CSV") != std::string::npos);
    ASSERT_TRUE(csv.find("28")    != std::string::npos);
    ASSERT_TRUE(csv.find("55")    != std::string::npos);
    ASSERT_TRUE(csv.find("INFO")  != std::string::npos);
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline map — SensorReading → string → MemoryQueueSink<string>
// ─────────────────────────────────────────────────────────────────────────────
TEST(Integration, Pipeline_MapToString) {
    auto source = std::make_shared<SimplePublisher<SensorReading>>();
    auto sink   = makeMemoryQueueSink<std::string>();

    auto sub = std::make_shared<LambdaSubscriber<std::string>>(
        [&](const std::string& s) { sink->onNext(s); },
        [](const std::exception&) {},
        [&]() { sink->onComplete(); }
    );

    // Pipeline: map SensorReading → to_string → sink
    auto stream = DataStream<SensorReading>(source)
        .map([](const SensorReading& r) { return to_string(r); });
    stream.subscribe(sub);

    source->emit(SensorReading{"S_MAP", 27.0, 58.0, Severity::DEBUG});
    source->emit(SensorReading{"S_MAP", 31.0, 62.0, Severity::WARN});
    source->complete();

    ASSERT_EQ(sink->size(), static_cast<size_t>(2));
    ASSERT_TRUE(sink->isCompleted());

    auto items = sink->getQueue();
    ASSERT_TRUE(items[0].find("DEBUG") != std::string::npos);
    ASSERT_TRUE(items[1].find("WARN")  != std::string::npos);
}
