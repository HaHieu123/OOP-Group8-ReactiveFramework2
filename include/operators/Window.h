#pragma once

#include "core/DataStream.h"
#include "core/Publisher.h"
#include "core/Subscriber.h"
#include "core/Subscription.h"

#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace iot_stream {

/**
 * @brief Lớp đại diện cho một Cửa sổ dữ liệu (Window) chứa tập hợp các phần tử gom nhóm.
 * 
 * @tparam T Kiểu dữ liệu của các phần tử trong cửa sổ.
 */
template <typename T>
class Window {
public:
    using TimePoint = std::chrono::system_clock::time_point;

    Window()
        : start_(std::chrono::system_clock::now()),
          end_(std::chrono::system_clock::now()) {}

    Window(TimePoint start, TimePoint end)
        : start_(start), end_(end) {}

    Window(TimePoint start, TimePoint end, std::vector<T> elements)
        : elements_(std::move(elements)), start_(start), end_(end) {}

    /**
     * @brief Thêm một phần tử vào cửa sổ.
     */
    void add(const T& element) {
        elements_.push_back(element);
    }

    /**
     * @brief Lấy số lượng phần tử hiện có trong cửa sổ.
     */
    size_t size() const {
        return elements_.size();
    }

    /**
     * @brief Kiểm tra cửa sổ có đang trống hay không.
     */
    bool empty() const {
        return elements_.empty();
    }

    auto begin() const { return elements_.begin(); }
    auto end() const { return elements_.end(); }

    auto begin() { return elements_.begin(); }
    auto end() { return elements_.end(); }

    TimePoint start() const { return start_; }
    TimePoint start_time() const { return start_; }
    TimePoint end_time() const { return end_; }

    void set_start(TimePoint t) { start_ = t; }
    void set_end(TimePoint t) { end_ = t; }

    /**
     * @brief Xóa toàn bộ phần tử trong cửa sổ để chuẩn bị chu kỳ mới.
     */
    void clear() {
        elements_.clear();
    }

    /**
     * @brief Lấy tham chiếu hằng tới danh sách các phần tử.
     */
    const std::vector<T>& elements() const {
        return elements_;
    }

private:
    std::vector<T> elements_;
    TimePoint start_;
    TimePoint end_;
};

// ==============================================================================
// Interface IWindowStrategy<T>
// ==============================================================================

/**
 * @brief Strategy Interface định nghĩa thuật toán kiểm soát đóng/mở cửa sổ dữ liệu.
 * 
 * @tparam T Kiểu dữ liệu phần tử luồng.
 */
template <typename T>
class IWindowStrategy {
public:
    virtual ~IWindowStrategy() = default;

    /**
     * @brief Kiểm tra xem cửa sổ hiện tại đã thỏa mãn điều kiện để phát (emit) hay chưa.
     * @param window Cửa sổ đang tích lũy dữ liệu.
     * @param now Thời điểm hiện tại (sử dụng std::chrono::steady_clock cho độ chính xác cao).
     * @return true nếu cần emit Window xuống downstream, false nếu tiếp tục tích lũy.
     */
    virtual bool shouldEmit(const Window<T>& window, std::chrono::steady_clock::time_point now) = 0;

    /**
     * @brief Được gọi mỗi khi có phần tử mới đi vào cửa sổ.
     * @param element Phần tử dữ liệu mới.
     */
    virtual void onElement(const T& element) = 0;

    /**
     * @brief Khởi tạo lại trạng thái chiến lược sau khi đã phát một Window.
     */
    virtual void reset() = 0;

    /**
     * @brief Cập nhật cửa sổ trước khi emit (ví dụ: tỉa các phần tử cũ trong SlidingWindow).
     */
    virtual void updateWindow(Window<T>& window, std::chrono::steady_clock::time_point now) {
        (void)window;
        (void)now;
    }
};

// ==============================================================================
// Concrete Window Strategies
// ==============================================================================

/*
================================================================================
ASCII Timeline Minh Họa: 1. TumblingWindow
================================================================================
Duration = 2000ms (Cửa sổ cố định, không gối đầu / non-overlapping)

Timeline:  0s             2s             4s             6s
Events:    ---e1---e2------|---e3---e4-----|---e5---------|
Windows:   [   Window 1   ][   Window 2   ][   Window 3   ]
Emit:      @ 2s: {e1, e2}   @ 4s: {e3, e4}  @ 6s: {e5}
================================================================================
*/
template <typename T>
class TumblingWindow : public IWindowStrategy<T> {
public:
    explicit TumblingWindow(std::chrono::milliseconds duration)
        : duration_(duration), has_start_time_(false) {}

    void onElement(const T& element) override {
        (void)element;
        if (!has_start_time_) {
            start_time_ = std::chrono::steady_clock::now();
            has_start_time_ = true;
        }
    }

    bool shouldEmit(const Window<T>& window, std::chrono::steady_clock::time_point now) override {
        if (!has_start_time_ || window.empty()) {
            return false;
        }
        return (now - start_time_) >= duration_;
    }

    void reset() override {
        has_start_time_ = false;
    }

private:
    std::chrono::milliseconds duration_;
    std::chrono::steady_clock::time_point start_time_;
    bool has_start_time_;
};

/*
================================================================================
ASCII Timeline Minh Họa: 2. SlidingWindow
================================================================================
WindowSize = 4s, SlideInterval = 2s (Cửa sổ trượt, có gối đầu / overlapping)

Timeline:  0s             2s             4s             6s
Events:    ---e1---e2------|---e3---e4-----|---e5---------|
Win 1:     [      {e1, e2, e3, e4}        ]
Win 2:                    [      {e3, e4, e5}           ]
Emit:      @ 4s: {e1, e2, e3, e4}          @ 6s: {e3, e4, e5}
================================================================================
*/
template <typename T>
class SlidingWindow : public IWindowStrategy<T> {
public:
    SlidingWindow(std::chrono::milliseconds windowSize, std::chrono::milliseconds slideInterval)
        : window_size_(windowSize),
          slide_interval_(slideInterval),
          has_start_(false) {}

    void onElement(const T& element) override {
        auto now = std::chrono::steady_clock::now();
        if (!has_start_) {
            last_slide_time_ = now;
            has_start_ = true;
        }
        history_.push_back({element, now});
    }

    void updateWindow(Window<T>& window, std::chrono::steady_clock::time_point now) override {
        auto cutoff = now - window_size_;
        // Dùng std::deque để pop_front các phần tử quá cũ (nằm ngoài phạm vi [now - windowSize, now])
        while (!history_.empty() && history_.front().second < cutoff) {
            history_.pop_front();
        }

        window.clear();
        for (const auto& item : history_) {
            window.add(item.first);
        }
    }

    bool shouldEmit(const Window<T>& window, std::chrono::steady_clock::time_point now) override {
        if (!has_start_ || window.empty()) {
            return false;
        }
        return (now - last_slide_time_) >= slide_interval_;
    }

    void reset() override {
        last_slide_time_ = std::chrono::steady_clock::now();
    }

private:
    std::chrono::milliseconds window_size_;
    std::chrono::milliseconds slide_interval_;
    std::chrono::steady_clock::time_point last_slide_time_;
    std::deque<std::pair<T, std::chrono::steady_clock::time_point>> history_;
    bool has_start_;
};

/*
================================================================================
ASCII Timeline Minh Họa: 3. CountWindow
================================================================================
Count = 3 phần tử (Gom nhóm theo số lượng, kích hoạt khi đủ N mẫu)

Timeline:  0s             1s             2s             3s
Events:    ---e1-----e2---------e3-------|---e4---e5----e6---|
Windows:   [  CountWindow 1: {e1, e2, e3}][CountWindow 2: {e4, e5, e6}]
Emit:      @ e3: Emit {e1, e2, e3}        @ e6: Emit {e4, e5, e6}
================================================================================
*/
template <typename T>
class CountWindow : public IWindowStrategy<T> {
public:
    explicit CountWindow(size_t count)
        : target_count_(count), current_count_(0) {}

    void onElement(const T& element) override {
        (void)element;
        current_count_++;
    }

    bool shouldEmit(const Window<T>& window, std::chrono::steady_clock::time_point now) override {
        (void)now;
        return window.size() >= target_count_ || current_count_ >= target_count_;
    }

    void reset() override {
        current_count_ = 0;
    }

private:
    size_t target_count_;
    size_t current_count_;
};

// ==============================================================================
// Window Publisher & Subscription Implementation
// ==============================================================================

namespace detail {

template <typename T>
class WindowSubscription : public Subscriber<T>,
                           public Subscription,
                           public std::enable_shared_from_this<WindowSubscription<T>> {
private:
    Subscriber<Window<T>>* downstream_;
    std::shared_ptr<IWindowStrategy<T>> strategy_;
    Subscription* upstream_{nullptr};
    Window<T> current_window_;
    std::mutex mtx_;
    bool cancelled_{false};

public:
    WindowSubscription(Subscriber<Window<T>>* downstream, std::shared_ptr<IWindowStrategy<T>> strategy)
        : downstream_(downstream), strategy_(std::move(strategy)) {
        current_window_.set_start(std::chrono::system_clock::now());
    }

    void onSubscribe(Subscription* sub) override {
        upstream_ = sub;
        if (downstream_) {
            downstream_->onSubscribe(this);
        }
    }

    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (cancelled_ || !downstream_ || !strategy_) return;

        auto now = std::chrono::steady_clock::now();
        strategy_->onElement(value);
        current_window_.add(value);
        strategy_->updateWindow(current_window_, now);

        if (strategy_->shouldEmit(current_window_, now)) {
            current_window_.set_end(std::chrono::system_clock::now());
            downstream_->onNext(current_window_);
            current_window_.clear();
            current_window_.set_start(std::chrono::system_clock::now());
            strategy_->reset();
        }
    }

    void onError(const std::exception& ex) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (cancelled_ || !downstream_) return;
        downstream_->onError(ex);
    }

    void onComplete() override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (cancelled_ || !downstream_) return;

        // Phát nốt các phần tử còn dư trong cửa sổ cuối cùng trước khi đóng luồng
        if (!current_window_.empty()) {
            current_window_.set_end(std::chrono::system_clock::now());
            downstream_->onNext(current_window_);
            current_window_.clear();
        }
        downstream_->onComplete();
    }

    void request(int n) override {
        if (upstream_) {
            upstream_->request(n);
        }
    }

    void cancel() override {
        std::lock_guard<std::mutex> lock(mtx_);
        cancelled_ = true;
        if (upstream_) {
            upstream_->cancel();
        }
    }
};

template <typename T>
class WindowPublisher : public Publisher<Window<T>> {
private:
    std::shared_ptr<Publisher<T>> source_;
    std::shared_ptr<IWindowStrategy<T>> strategy_;
    std::mutex mtx_;
    std::vector<std::shared_ptr<WindowSubscription<T>>> active_subs_;

public:
    WindowPublisher(std::shared_ptr<Publisher<T>> source, std::shared_ptr<IWindowStrategy<T>> strategy)
        : source_(std::move(source)), strategy_(std::move(strategy)) {}

    void subscribe(Subscriber<Window<T>>* subscriber) override {
        if (!subscriber || !source_ || !strategy_) return;
        auto sub = std::make_shared<WindowSubscription<T>>(subscriber, strategy_);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            active_subs_.push_back(sub);
        }
        source_->subscribe(sub.get());
    }
};

} // namespace detail

// ==============================================================================
// Hiện thực phương thức DataStream<T>::window()
// ==============================================================================

template <typename T>
template <typename Strategy>
inline DataStream<Window<T>> DataStream<T>::window(std::shared_ptr<Strategy> windowStrategy) {
    std::shared_ptr<IWindowStrategy<T>> base_strategy = windowStrategy;
    auto winPub = std::make_shared<detail::WindowPublisher<T>>(source_, base_strategy);
    return DataStream<Window<T>>(winPub);
}

} // namespace iot_stream

/*
================================================================================
Ví dụ sử dụng Module Windowing trong Pipeline Fluent API:
================================================================================

#include "operators/Window.h"
#include "core/SimplePublisher.h"
#include "sinks/IDataSink.h"
#include <iostream>
#include <chrono>

using namespace std::chrono_literals;
using namespace iot_stream;

void example_window_pipeline() {
    auto source = std::make_shared<SimplePublisher<int>>();

    // Pipeline: Nhận số nguyên -> Gom cụm 3 phần tử -> In kích thước từng window
    auto stream = DataStream<int>(source)
        .window(std::make_shared<CountWindow<int>>(3))
        .map([](const Window<int>& win) {
            return "Window emitted with " + std::to_string(win.size()) + " elements.";
        });

    // Phát các phần tử
    for (int i = 1; i <= 6; ++i) {
        source->emit(i); // Cứ mỗi 3 phần tử, một Window sẽ được emit
    }
}
================================================================================
*/
