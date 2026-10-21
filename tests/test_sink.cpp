/**
 * @file test_sink.cpp
 * @brief Kiểm thử module Sinks: MemoryQueueSink, FileSink, ConsoleSink, WebSocketSink.
 *
 * Test cases:
 *   Sink::MemoryQueue_StoreAndRetrieve  — lưu trữ và truy xuất phần tử đúng thứ tự
 *   Sink::MemoryQueue_ClearAndEmpty     — clear() + empty() hoạt động
 *   Sink::MemoryQueue_CompletedFlag     — isCompleted() set sau onComplete
 *   Sink::MemoryQueue_ErrorFlag         — hasError() set sau onError
 *   Sink::FileSink_CsvOutput            — ghi CSV + đọc lại kiểm tra header và data
 *   Sink::WebSocketSink_JsonFrames      — stub callback nhận đủ frame onNext + onComplete
 *   Sink::ConsoleSink_SmokeSensorReading — smoke test: không throw
 */

#include "test_framework.h"

#include "sinks/ConsoleSink.h"
#include "sinks/FileSink.h"
#include "sinks/MemoryQueueSink.h"
#include "sinks/WebSocketSink.h"
#include "source/Sensor.h"

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

using namespace iot_stream;

// ─────────────────────────────────────────────────────────────────────────────
// MemoryQueueSink — lưu trữ đúng thứ tự và kích thước
// ─────────────────────────────────────────────────────────────────────────────
TEST(Sink, MemoryQueue_StoreAndRetrieve) {
    auto sink = makeMemoryQueueSink<int>();

    sink->onNext(10);
    sink->onNext(20);
    sink->onNext(30);

    ASSERT_EQ(sink->size(), static_cast<size_t>(3));
    ASSERT_FALSE(sink->empty());

    auto items = sink->getQueue();
    ASSERT_EQ(items.size(), static_cast<size_t>(3));
    ASSERT_EQ(items[0], 10);
    ASSERT_EQ(items[1], 20);
    ASSERT_EQ(items[2], 30);
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryQueueSink — clear() xóa hàng đợi, empty() đúng
// ─────────────────────────────────────────────────────────────────────────────
TEST(Sink, MemoryQueue_ClearAndEmpty) {
    auto sink = makeMemoryQueueSink<int>();
    sink->onNext(1);
    sink->onNext(2);
    ASSERT_FALSE(sink->empty());

    sink->clear();
    ASSERT_EQ(sink->size(), static_cast<size_t>(0));
    ASSERT_TRUE(sink->empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryQueueSink — isCompleted() flag set đúng
// ─────────────────────────────────────────────────────────────────────────────
TEST(Sink, MemoryQueue_CompletedFlag) {
    auto sink = makeMemoryQueueSink<int>();
    ASSERT_FALSE(sink->isCompleted());

    sink->onNext(42);
    sink->onComplete();

    ASSERT_TRUE(sink->isCompleted());
    ASSERT_EQ(sink->size(), static_cast<size_t>(1)); // phần tử vẫn còn
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryQueueSink — hasError() flag set đúng
// ─────────────────────────────────────────────────────────────────────────────
TEST(Sink, MemoryQueue_ErrorFlag) {
    auto sink = makeMemoryQueueSink<std::string>();
    ASSERT_FALSE(sink->hasError());

    sink->onError(StreamException("pipeline_error"));
    ASSERT_TRUE(sink->hasError());
}

// ─────────────────────────────────────────────────────────────────────────────
// FileSink — ghi CSV và đọc lại kiểm tra header + dữ liệu hàng
// ─────────────────────────────────────────────────────────────────────────────
TEST(Sink, FileSink_CsvOutput) {
    const std::string test_csv = "_test_file_sink_output.csv";
    std::remove(test_csv.c_str()); // xóa file cũ nếu có

    {
        auto file_sink = makeFileSink<SensorReading>(test_csv, true);
        ASSERT_TRUE(file_sink->is_open());

        SensorReading r1{"S1", 25.5, 60.0, Severity::INFO};
        SensorReading r2{"S2", 42.0, 55.0, Severity::WARN};
        file_sink->onNext(r1);
        file_sink->onNext(r2);
        file_sink->flush();
        file_sink->close();
    }

    // Đọc lại file CSV và kiểm tra nội dung
    std::ifstream ifs(test_csv);
    ASSERT_TRUE(ifs.is_open());

    std::string line;
    std::getline(ifs, line); // dòng header
    ASSERT_TRUE(line.find("timestamp") != std::string::npos);

    std::getline(ifs, line); // dòng dữ liệu 1
    ASSERT_TRUE(line.find("S1") != std::string::npos);
    ASSERT_TRUE(line.find("INFO") != std::string::npos);

    std::getline(ifs, line); // dòng dữ liệu 2
    ASSERT_TRUE(line.find("S2") != std::string::npos);
    ASSERT_TRUE(line.find("WARN") != std::string::npos);

    ifs.close();
    std::remove(test_csv.c_str()); // dọn dẹp
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocketSink — stub callback: nhận đúng số JSON frame
// onNext → 1 frame data, onComplete → 1 frame {"type":"complete"}
// ─────────────────────────────────────────────────────────────────────────────
TEST(Sink, WebSocketSink_JsonFrames) {
    std::vector<std::string> json_frames;

    auto ws_sink = makeWebSocketSink<SensorReading>([&](const std::string& frame) {
        json_frames.push_back(frame);
    });

    SensorReading r{1700000005000LL, "S_WS", 36.6, 70.0, Severity::ERROR};
    ws_sink->onNext(r);
    ws_sink->onComplete();

    // 1 frame cho onNext + 1 frame cho onComplete
    ASSERT_EQ(json_frames.size(), static_cast<size_t>(2));

    // Frame dữ liệu phải chứa sensorId và severity
    ASSERT_TRUE(json_frames[0].find("S_WS")  != std::string::npos);
    ASSERT_TRUE(json_frames[0].find("ERROR") != std::string::npos);

    // Frame complete
    ASSERT_TRUE(json_frames[1].find("complete") != std::string::npos);
}

// ─────────────────────────────────────────────────────────────────────────────
// ConsoleSink — smoke test: onNext(SensorReading) không ném ngoại lệ
// ─────────────────────────────────────────────────────────────────────────────
TEST(Sink, ConsoleSink_SmokeSensorReading) {
    auto console_sink = makeConsoleSink<SensorReading>(false); // không in timestamp
    SensorReading r{1700000000000LL, "S_CONSOLE", 28.3, 50.0, Severity::INFO};

    // Phải không ném bất kỳ ngoại lệ nào
    bool threw = false;
    try {
        console_sink->onNext(r);
        console_sink->flush();
    } catch (...) {
        threw = true;
    }
    ASSERT_FALSE(threw);
}
