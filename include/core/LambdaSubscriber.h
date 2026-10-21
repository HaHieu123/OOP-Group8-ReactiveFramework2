#pragma once

#include "core/Subscriber.h"
#include "core/Subscription.h"

#include <exception>
#include <functional>
#include <limits>
#include <mutex>

namespace iot_stream {

/**
 * @brief Concrete Subscriber linh hoạt, khởi tạo bằng các hàm lambda hoặc std::function.
 * 
 * Cho phép tạo nhanh một Subscriber mà không cần viết class kế thừa từ Subscriber<T>:
 *   auto sub = std::make_shared<LambdaSubscriber<int>>(
 *       [](int val) { std::cout << "Received: " << val << "\n"; },
 *       [](const std::exception& e) { std::cerr << "Error: " << e.what() << "\n"; },
 *       []() { std::cout << "Stream completed!\n"; }
 *   );
 * 
 * @tparam T Kiểu dữ liệu sự kiện nhận từ luồng.
 */
template <typename T>
class LambdaSubscriber : public Subscriber<T> {
public:
    using NextFn = std::function<void(const T&)>;
    using ErrorFn = std::function<void(const std::exception&)>;
    using CompleteFn = std::function<void()>;

    /**
     * @brief Khởi tạo LambdaSubscriber.
     * @param onNext Callback khi có dữ liệu mới.
     * @param onError Callback khi xảy ra lỗi (tùy chọn).
     * @param onComplete Callback khi luồng kết thúc (tùy chọn).
     * @param auto_request_max Nếu true, tự động gọi request(INT_MAX) trong onSubscribe (Unbounded).
     */
    explicit LambdaSubscriber(
        NextFn onNext,
        ErrorFn onError = nullptr,
        CompleteFn onComplete = nullptr,
        bool auto_request_max = true)
        : onNext_(std::move(onNext)),
          onError_(std::move(onError)),
          onComplete_(std::move(onComplete)),
          auto_request_max_(auto_request_max) {}

    /**
     * @brief Được gọi khi kết nối thành công với Publisher.
     * Lưu con trỏ Subscription và tự động request dữ liệu nếu auto_request_max = true.
     */
    void onSubscribe(Subscription* sub) override {
        std::lock_guard<std::mutex> lock(mtx_);
        sub_ = sub;
        if (sub_ && auto_request_max_) {
            sub_->request(std::numeric_limits<int>::max());
        }
    }

    /**
     * @brief Nhận dữ liệu tiếp theo từ Publisher và chuyển vào onNext_ callback.
     */
    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (is_completed_ || !onNext_) return;
        onNext_(value);
    }

    /**
     * @brief Nhận thông báo lỗi từ Publisher và chuyển vào onError_ callback.
     */
    void onError(const std::exception& ex) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (is_completed_) return;
        is_completed_ = true;
        if (onError_) {
            onError_(ex);
        }
    }

    /**
     * @brief Nhận tín hiệu kết thúc luồng và chuyển vào onComplete_ callback.
     */
    void onComplete() override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (is_completed_) return;
        is_completed_ = true;
        if (onComplete_) {
            onComplete_();
        }
    }

    /**
     * @brief Yêu cầu Publisher phát thêm n phần tử dữ liệu (nếu auto_request_max = false).
     */
    void request(int n) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (sub_) {
            sub_->request(n);
        }
    }

    /**
     * @brief Hủy đăng ký nhận dữ liệu từ luồng.
     */
    void cancel() {
        std::lock_guard<std::mutex> lock(mtx_);
        if (sub_) {
            sub_->cancel();
            sub_ = nullptr;
        }
        is_completed_ = true;
    }

    /**
     * @brief Lấy con trỏ Subscription hiện tại.
     */
    Subscription* get_subscription() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return sub_;
    }

private:
    NextFn onNext_;
    ErrorFn onError_;
    CompleteFn onComplete_;
    bool auto_request_max_{true};
    Subscription* sub_{nullptr};
    bool is_completed_{false};
    mutable std::mutex mtx_;
};

} // namespace iot_stream

