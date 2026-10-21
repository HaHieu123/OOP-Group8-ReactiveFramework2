/**
 * @file test_core.cpp
 * @brief Kiểm thử Core module: SimplePublisher + LambdaSubscriber.
 *
 * Test cases:
 *   Core::OnePublisher_TwoSubscribers  — 1 publisher, 2 subscriber, emit 100 phần tử
 *   Core::SubscriberCount              — subscriber_count() đúng trước/sau cancel
 *   Core::ErrorPropagation             — publisher.error() gọi onError trên subscriber
 *   Core::NoEmitAfterComplete          — emit sau complete bị bỏ qua
 *   Core::SeverityOrdering             — kiểm tra operator>= của Severity
 */

#include "test_framework.h"

#include "core/Exceptions.h"
#include "core/LambdaSubscriber.h"
#include "core/Severity.h"
#include "core/SimplePublisher.h"

#include <vector>

using namespace iot_stream;

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: 1 publisher + 2 subscriber nhận đủ 100 phần tử theo đúng thứ tự
// ─────────────────────────────────────────────────────────────────────────────
TEST(Core, OnePublisher_TwoSubscribers) {
    auto publisher = std::make_shared<SimplePublisher<int>>();

    std::vector<int> sub1_received;
    std::vector<int> sub2_received;
    bool sub1_completed = false;
    bool sub2_completed = false;

    auto sub1 = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { sub1_received.push_back(val); },
        [](const std::exception&) {},
        [&]() { sub1_completed = true; }
    );
    auto sub2 = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { sub2_received.push_back(val); },
        [](const std::exception&) {},
        [&]() { sub2_completed = true; }
    );

    publisher->subscribe(sub1.get());
    publisher->subscribe(sub2.get());
    ASSERT_EQ(publisher->subscriber_count(), static_cast<size_t>(2));

    const int N = 100;
    for (int i = 0; i < N; ++i) publisher->emit(i);
    publisher->complete();

    // Cả hai subscriber phải nhận đủ 100 phần tử theo đúng thứ tự
    ASSERT_EQ(sub1_received.size(), static_cast<size_t>(100));
    ASSERT_EQ(sub2_received.size(), static_cast<size_t>(100));
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(sub1_received[i], i);
        ASSERT_EQ(sub2_received[i], i);
    }
    ASSERT_TRUE(sub1_completed);
    ASSERT_TRUE(sub2_completed);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: subscriber_count() phản ánh đúng số lượng subscriber
// ─────────────────────────────────────────────────────────────────────────────
TEST(Core, SubscriberCount) {
    auto pub = std::make_shared<SimplePublisher<int>>();

    auto sub1 = std::make_shared<LambdaSubscriber<int>>(
        [](int) {}, [](const std::exception&) {}, []() {}
    );
    auto sub2 = std::make_shared<LambdaSubscriber<int>>(
        [](int) {}, [](const std::exception&) {}, []() {}
    );

    ASSERT_EQ(pub->subscriber_count(), static_cast<size_t>(0));
    pub->subscribe(sub1.get());
    ASSERT_EQ(pub->subscriber_count(), static_cast<size_t>(1));
    pub->subscribe(sub2.get());
    ASSERT_EQ(pub->subscriber_count(), static_cast<size_t>(2));
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: publisher.error() phát tín hiệu lỗi đến subscriber
// ─────────────────────────────────────────────────────────────────────────────
TEST(Core, ErrorPropagation) {
    auto pub = std::make_shared<SimplePublisher<int>>();
    bool error_received = false;
    std::string error_msg;

    auto sub = std::make_shared<LambdaSubscriber<int>>(
        [](int) {},
        [&](const std::exception& ex) {
            error_received = true;
            error_msg = ex.what();
        },
        []() {}
    );
    pub->subscribe(sub.get());
    pub->error(StreamException("sensor_failure"));

    ASSERT_TRUE(error_received);
    ASSERT_TRUE(error_msg.find("sensor_failure") != std::string::npos);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: emit sau complete phải bị bỏ qua (publisher đã terminated)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Core, NoEmitAfterComplete) {
    auto pub = std::make_shared<SimplePublisher<int>>();
    int count = 0;

    auto sub = std::make_shared<LambdaSubscriber<int>>(
        [&](int) { ++count; },
        [](const std::exception&) {},
        []() {}
    );
    pub->subscribe(sub.get());
    pub->emit(1);
    pub->complete();
    pub->emit(2); // phải bị bỏ qua vì đã terminated
    pub->emit(3);

    ASSERT_EQ(count, 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 5: Severity enum ordering (operator>=)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Core, SeverityOrdering) {
    ASSERT_TRUE(Severity::WARN  >= Severity::INFO);
    ASSERT_TRUE(Severity::ERROR >= Severity::WARN);
    ASSERT_TRUE(Severity::FATAL >= Severity::ERROR);
    ASSERT_FALSE(Severity::INFO >= Severity::WARN);
    ASSERT_FALSE(Severity::DEBUG >= Severity::INFO);

    // to_string
    ASSERT_EQ(to_string(Severity::INFO),  std::string("INFO"));
    ASSERT_EQ(to_string(Severity::WARN),  std::string("WARN"));
    ASSERT_EQ(to_string(Severity::ERROR), std::string("ERROR"));
    ASSERT_EQ(to_string(Severity::FATAL), std::string("FATAL"));
    ASSERT_EQ(to_string(Severity::DEBUG), std::string("DEBUG"));
}
