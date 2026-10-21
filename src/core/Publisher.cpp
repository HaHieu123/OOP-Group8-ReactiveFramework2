#include "core/SimplePublisher.h"
#include <cstdint>
#include <string>

namespace iot_stream {

// ==============================================================================
// Explicit Template Instantiations
// Đảm bảo các kiểu dữ liệu phổ biến được biên dịch sẵn vào iot_stream static library
// ==============================================================================

template class SimplePublisher<int>;
template class SimplePublisher<double>;
template class SimplePublisher<float>;
template class SimplePublisher<int64_t>;
template class SimplePublisher<std::string>;

template class SimpleSubscription<int>;
template class SimpleSubscription<double>;
template class SimpleSubscription<float>;
template class SimpleSubscription<int64_t>;
template class SimpleSubscription<std::string>;

} // namespace iot_stream

/*
================================================================================
Unit Test Mô Tả (Verification Specification):
Kiểm thử 1 Publisher + 2 Subscribers đồng thời, emit 100 phần tử -> Cả 2 nhận đủ.
================================================================================

#include "core/SimplePublisher.h"
#include "core/LambdaSubscriber.h"
#include <cassert>
#include <iostream>
#include <vector>

void run_publisher_subscriber_test() {
    using namespace iot_stream;

    // Khởi tạo Publisher
    auto publisher = std::make_shared<SimplePublisher<int>>();

    std::vector<int> sub1_events;
    std::vector<int> sub2_events;
    bool sub1_finished = false;
    bool sub2_finished = false;

    // Subscriber 1 thu thập dữ liệu
    auto sub1 = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { sub1_events.push_back(val); },
        [](const std::exception&) {},
        [&]() { sub1_finished = true; }
    );

    // Subscriber 2 thu thập dữ liệu độc lập
    auto sub2 = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { sub2_events.push_back(val); },
        [](const std::exception&) {},
        [&]() { sub2_finished = true; }
    );

    publisher->subscribe(sub1.get());
    publisher->subscribe(sub2.get());

    assert(publisher->subscriber_count() == 2);

    // Phát 100 phần tử
    const int TOTAL_ITEMS = 100;
    for (int i = 0; i < TOTAL_ITEMS; ++i) {
        publisher->emit(i);
    }
    publisher->complete();

    // Kiểm tra kết quả
    assert(sub1_events.size() == 100);
    assert(sub2_events.size() == 100);
    for (int i = 0; i < TOTAL_ITEMS; ++i) {
        assert(sub1_events[i] == i);
        assert(sub2_events[i] == i);
    }
    assert(sub1_finished);
    assert(sub2_finished);

    std::cout << "[PASS] 1 Publisher + 2 Subscribers received all 100 items.\n";
}
================================================================================
*/

