/**
 * @file test_backpressure.cpp
 * @brief Kiểm thử module Backpressure — 4 chiến lược: BUFFER, DROP, LATEST, ERROR.
 *
 * Test cases:
 *   Backpressure::Buffer_HoldsAndFlushes  — buffer giữ và flush khi có demand
 *   Backpressure::Buffer_DropsOverCapacity — buffer overflow → droppedCount tăng
 *   Backpressure::Drop_IgnoresWithoutDemand — DROP bỏ khi không có demand
 *   Backpressure::Latest_KeepsNewest        — LATEST chỉ giữ giá trị mới nhất
 *   Backpressure::Error_ThrowsOnOverflow    — ERROR gọi onError khi tràn
 *   Backpressure::Cancel_StopsDelivery      — cancel() ngăn onNext tiếp theo
 */

#include "test_framework.h"

#include "backpressure/Backpressure.h"
#include "core/Exceptions.h"
#include "core/LambdaSubscriber.h"

#include <vector>

using namespace iot_stream;

// ─────────────────────────────────────────────────────────────────────────────
// BUFFER — giữ đúng phần tử và flush khi subscriber request(n)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Backpressure, Buffer_HoldsAndFlushes) {
    std::vector<int> received;
    auto sub = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { received.push_back(val); },
        [](const std::exception&) {},
        []() {},
        false // auto_request_max = false → kiểm thử backpressure thủ công
    );

    // capacity = 5: đủ chứa cả 3 phần tử
    auto bp_sub = makeBackpressureSubscription<int>(
        BackpressureStrategy::BUFFER, 5, sub.get()
    );
    sub->onSubscribe(bp_sub.get());

    // Chưa có demand: phát 3 phần tử → buffer đầy, chưa giao
    bp_sub->onNext(1);
    bp_sub->onNext(2);
    bp_sub->onNext(3);
    ASSERT_TRUE(received.empty());
    ASSERT_EQ(bp_sub->bufferedCount(), static_cast<size_t>(3));

    // Request 2 phần tử đầu
    bp_sub->request(2);
    ASSERT_EQ(received.size(), static_cast<size_t>(2));
    ASSERT_EQ(received[0], 1);
    ASSERT_EQ(received[1], 2);
    ASSERT_EQ(bp_sub->bufferedCount(), static_cast<size_t>(1));

    // Request 1 phần tử cuối
    bp_sub->request(1);
    ASSERT_EQ(received.size(), static_cast<size_t>(3));
    ASSERT_EQ(received[2], 3);
    ASSERT_EQ(bp_sub->bufferedCount(), static_cast<size_t>(0));
}

// ─────────────────────────────────────────────────────────────────────────────
// BUFFER — khi buffer đầy, các phần tử thừa bị đếm vào droppedCount
// ─────────────────────────────────────────────────────────────────────────────
TEST(Backpressure, Buffer_DropsOverCapacity) {
    std::vector<int> received;
    auto sub = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { received.push_back(val); },
        [](const std::exception&) {},
        []() {},
        false
    );

    // capacity = 3: chỉ chứa được 3 phần tử
    auto bp_sub = makeBackpressureSubscription<int>(
        BackpressureStrategy::BUFFER, 3, sub.get()
    );
    sub->onSubscribe(bp_sub.get());

    // Phát 5 phần tử vào buffer capacity=3 → 3 buffered, 2 dropped
    bp_sub->onNext(1);
    bp_sub->onNext(2);
    bp_sub->onNext(3);
    bp_sub->onNext(4); // overflow → dropped
    bp_sub->onNext(5); // overflow → dropped

    ASSERT_TRUE(received.empty());
    ASSERT_EQ(bp_sub->bufferedCount(), static_cast<size_t>(3));
    ASSERT_EQ(bp_sub->droppedCount(),  static_cast<size_t>(2));
}

// ─────────────────────────────────────────────────────────────────────────────
// DROP — bỏ phần tử khi không có demand, giữ phần tử khi có demand
// ─────────────────────────────────────────────────────────────────────────────
TEST(Backpressure, Drop_IgnoresWithoutDemand) {
    std::vector<int> received;
    auto sub = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { received.push_back(val); },
        [](const std::exception&) {},
        []() {},
        false
    );

    auto bp_sub = makeBackpressureSubscription<int>(
        BackpressureStrategy::DROP, 10, sub.get()
    );
    sub->onSubscribe(bp_sub.get());

    bp_sub->request(1); // demand = 1
    bp_sub->onNext(100); // giao ngay (demand → 0)
    bp_sub->onNext(200); // DROP (demand = 0)
    bp_sub->onNext(300); // DROP (demand = 0)

    ASSERT_EQ(received.size(), static_cast<size_t>(1));
    ASSERT_EQ(received[0], 100);
    ASSERT_EQ(bp_sub->droppedCount(), static_cast<size_t>(2));
}

// ─────────────────────────────────────────────────────────────────────────────
// LATEST — khi không có demand, chỉ giữ giá trị MỚI NHẤT
// ─────────────────────────────────────────────────────────────────────────────
TEST(Backpressure, Latest_KeepsNewest) {
    std::vector<int> received;
    auto sub = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { received.push_back(val); },
        [](const std::exception&) {},
        []() {},
        false
    );

    auto bp_sub = makeBackpressureSubscription<int>(
        BackpressureStrategy::LATEST, 1, sub.get()
    );
    sub->onSubscribe(bp_sub.get());

    bp_sub->request(1); // demand = 1
    bp_sub->onNext(1);  // giao ngay (demand → 0)

    // Không có demand: 3 phần tử ghi đè nhau, chỉ giữ 4 (mới nhất)
    // droppedCount tăng 2 lần (2 → 3 → 4: 2 và 3 bị overwrite)
    bp_sub->onNext(2);
    bp_sub->onNext(3);
    bp_sub->onNext(4);

    ASSERT_EQ(received.size(), static_cast<size_t>(1));
    ASSERT_EQ(bp_sub->droppedCount(), static_cast<size_t>(2));

    // Request phần tử tiếp → nhận được 4 (mới nhất)
    bp_sub->request(1);
    ASSERT_EQ(received.size(), static_cast<size_t>(2));
    ASSERT_EQ(received[1], 4);
}

// ─────────────────────────────────────────────────────────────────────────────
// ERROR — khi không có demand và onNext được gọi → onError(BackpressureOverflowException)
// ─────────────────────────────────────────────────────────────────────────────
TEST(Backpressure, Error_ThrowsOnOverflow) {
    std::vector<int> received;
    bool caught_error = false;

    auto sub = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { received.push_back(val); },
        [&](const std::exception& ex) {
            caught_error = true;
            (void)ex;
        },
        []() {},
        false
    );

    auto bp_sub = makeBackpressureSubscription<int>(
        BackpressureStrategy::ERROR, 0, sub.get()
    );
    sub->onSubscribe(bp_sub.get());

    bp_sub->request(1);  // demand = 1
    bp_sub->onNext(10);  // giao ngay (demand → 0)
    bp_sub->onNext(20);  // demand = 0 → ném lỗi qua onError

    ASSERT_EQ(received.size(), static_cast<size_t>(1));
    ASSERT_EQ(received[0], 10);
    ASSERT_TRUE(caught_error);
}

// ─────────────────────────────────────────────────────────────────────────────
// Cancel — sau khi cancel(), onNext tiếp theo phải bị bỏ qua
// ─────────────────────────────────────────────────────────────────────────────
TEST(Backpressure, Cancel_StopsDelivery) {
    std::vector<int> received;
    auto sub = std::make_shared<LambdaSubscriber<int>>(
        [&](int val) { received.push_back(val); },
        [](const std::exception&) {},
        []() {},
        false
    );

    auto bp_sub = makeBackpressureSubscription<int>(
        BackpressureStrategy::BUFFER, 100, sub.get()
    );
    sub->onSubscribe(bp_sub.get());

    bp_sub->request(10); // demand = 10
    bp_sub->onNext(1);   // giao
    bp_sub->onNext(2);   // giao
    bp_sub->cancel();    // hủy đăng ký
    bp_sub->onNext(3);   // phải bị bỏ qua

    ASSERT_EQ(received.size(), static_cast<size_t>(2));
    ASSERT_EQ(received[0], 1);
    ASSERT_EQ(received[1], 2);
}
