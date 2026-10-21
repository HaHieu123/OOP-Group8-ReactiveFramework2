#pragma once

#include "core/Publisher.h"
#include "core/Subscriber.h"
#include "core/Subscription.h"

#include <algorithm>
#include <exception>
#include <memory>
#include <mutex>
#include <vector>

namespace iot_stream {

template <typename T>
class SimplePublisher;

/**
 * @brief Subscription đơn giản kết nối giữa SimplePublisher và Subscriber.
 * Quản lý vòng đời và cho phép Subscriber hủy đăng ký (cancel) an toàn.
 */
template <typename T>
class SimpleSubscription : public Subscription {
private:
    SimplePublisher<T>* publisher_;
    Subscriber<T>* subscriber_;
    std::mutex mtx_;
    bool cancelled_{false};

public:
    SimpleSubscription(SimplePublisher<T>* publisher, Subscriber<T>* subscriber)
        : publisher_(publisher), subscriber_(subscriber) {}

    void request(int n) override {
        // Mô hình Push-based: request(n) biểu thị nhu cầu từ phía subscriber
        (void)n;
    }

    void cancel() override;
};

/**
 * @brief Concrete Publisher cài đặt theo mô hình Push-based (Đẩy dữ liệu thời gian thực).
 * 
 * Nguyên lý hoạt động (Push-based model):
 * - SimplePublisher nắm giữ danh sách các Subscriber đã đăng ký (subscribers_).
 * - Khi emit(value) được gọi, Publisher chủ động duyệt và đẩy phần tử tới từng Subscriber
 *   qua hàm sub->onNext(value).
 * - Nếu Subscriber ném ngoại lệ trong onNext(), SimplePublisher sẽ bắt ngoại lệ và gọi sub->onError(ex),
 *   đồng thời tự động loại bỏ Subscriber bị lỗi khỏi danh sách phát sóng.
 * 
 * Tính an toàn đa luồng (Thread-Safety):
 * - Các phương thức subscribe(), emit(), complete(), error() có thể được gọi đồng thời
 *   từ nhiều luồng khác nhau (ví dụ: Producer thread gọi emit(), trong khi UI thread gọi subscribe()).
 * - Danh sách subscribers được bảo vệ bằng std::mutex. Để ngăn ngừa deadlock và iterator invalidation,
 *   khi emit(), danh sách subscriber được snapshot ra bản sao tạm trước khi dispatch.
 * 
 * @tparam T Kiểu dữ liệu của các phần tử luồng (Stream Event).
 */
template <typename T>
class SimplePublisher : public Publisher<T> {
public:
    SimplePublisher() = default;
    ~SimplePublisher() override = default;

    /**
     * @brief Đăng ký một Subscriber mới để nhận dữ liệu từ luồng.
     * @param subscriber Con trỏ tới Subscriber.
     */
    void subscribe(Subscriber<T>* subscriber) override {
        if (!subscriber) return;

        SimpleSubscription<T>* sub_ptr = nullptr;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (is_terminated_) return;

            subscribers_.push_back(subscriber);
            auto subscription = std::make_unique<SimpleSubscription<T>>(this, subscriber);
            sub_ptr = subscription.get();
            subscriptions_.push_back(std::move(subscription));
        }

        // Kích hoạt onSubscribe bên ngoài lock để tránh deadlock reentrancy
        subscriber->onSubscribe(sub_ptr);
    }

    /**
     * @brief Phát một phần tử dữ liệu mới tới tất cả subscribers đang hoạt động.
     * @param value Giá trị sự kiện mới cần phát.
     */
    void emit(const T& value) {
        std::vector<Subscriber<T>*> targets;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (is_terminated_) return;
            targets = subscribers_; // Snapshot an toàn
        }

        std::vector<Subscriber<T>*> failed_subs;
        for (auto* sub : targets) {
            if (!sub) continue;
            try {
                sub->onNext(value);
            } catch (const std::exception& ex) {
                try {
                    sub->onError(ex);
                } catch (...) {}
                failed_subs.push_back(sub);
            }
        }

        if (!failed_subs.empty()) {
            std::lock_guard<std::mutex> lock(mtx_);
            for (auto* failed : failed_subs) {
                auto it = std::find(subscribers_.begin(), subscribers_.end(), failed);
                if (it != subscribers_.end()) {
                    subscribers_.erase(it);
                }
            }
        }
    }

    /**
     * @brief Phát tín hiệu hoàn thành luồng tới tất cả subscribers.
     */
    void complete() {
        std::vector<Subscriber<T>*> targets;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (is_terminated_) return;
            is_terminated_ = true;
            targets = std::move(subscribers_);
            subscribers_.clear();
        }

        for (auto* sub : targets) {
            if (sub) {
                try {
                    sub->onComplete();
                } catch (...) {}
            }
        }
    }

    /**
     * @brief Phát tín hiệu lỗi nghiêm trọng tới tất cả subscribers.
     * @param ex Ngoại lệ mô tả chi tiết lỗi phát sinh.
     */
    void error(const std::exception& ex) {
        std::vector<Subscriber<T>*> targets;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (is_terminated_) return;
            is_terminated_ = true;
            targets = std::move(subscribers_);
            subscribers_.clear();
        }

        for (auto* sub : targets) {
            if (sub) {
                try {
                    sub->onError(ex);
                } catch (...) {}
            }
        }
    }

    /**
     * @brief Lấy số lượng subscriber hiện đang hoạt động.
     */
    size_t subscriber_count() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return subscribers_.size();
    }

    /**
     * @brief Xóa một subscriber khỏi danh sách (được gọi từ SimpleSubscription::cancel).
     */
    void remove_subscriber(Subscriber<T>* subscriber) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = std::find(subscribers_.begin(), subscribers_.end(), subscriber);
        if (it != subscribers_.end()) {
            subscribers_.erase(it);
        }
    }

private:
    mutable std::mutex mtx_;
    std::vector<Subscriber<T>*> subscribers_;
    std::vector<std::unique_ptr<SimpleSubscription<T>>> subscriptions_;
    bool is_terminated_{false};
};

template <typename T>
inline void SimpleSubscription<T>::cancel() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (!cancelled_) {
        cancelled_ = true;
        if (publisher_) {
            publisher_->remove_subscriber(subscriber_);
        }
    }
}

} // namespace iot_stream

/*
================================================================================
Unit Test Mô Tả:
1 SimplePublisher + 2 LambdaSubscribers, emit 100 phần tử -> Cả 2 nhận đủ 100
================================================================================

#include "core/SimplePublisher.h"
#include "core/LambdaSubscriber.h"
#include <cassert>
#include <iostream>
#include <vector>

void test_one_publisher_multiple_subscribers() {
    using namespace iot_stream;

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

    assert(publisher->subscriber_count() == 2);

    // Phát 100 phần tử
    const int N = 100;
    for (int i = 0; i < N; ++i) {
        publisher->emit(i);
    }
    publisher->complete();

    // Xác nhận kết quả
    assert(sub1_received.size() == 100);
    assert(sub2_received.size() == 100);
    for (int i = 0; i < N; ++i) {
        assert(sub1_received[i] == i);
        assert(sub2_received[i] == i);
    }
    assert(sub1_completed);
    assert(sub2_completed);

    std::cout << "[SUCCESS] 1 publisher + 2 subscribers received all 100 items.\n";
}
================================================================================
*/

