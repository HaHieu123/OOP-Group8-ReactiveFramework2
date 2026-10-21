#pragma once

#include <limits>
#include <mutex>
#include <utility>
#include <vector>

namespace iot_stream {

namespace detail {

// ==============================================================================
// 1. Transform Operator (map): Chuyển đổi T -> R
// ==============================================================================

template <typename T, typename R>
class TransformSubscription : public Subscriber<T>,
                              public Subscription,
                              public std::enable_shared_from_this<TransformSubscription<T, R>> {
private:
    Subscriber<R>* downstream_;
    std::function<R(const T&)> mapper_;
    Subscription* upstream_{nullptr};
    std::mutex mtx_;
    bool cancelled_{false};

public:
    TransformSubscription(Subscriber<R>* downstream, std::function<R(const T&)> mapper)
        : downstream_(downstream), mapper_(std::move(mapper)) {}

    void onSubscribe(Subscription* sub) override {
        upstream_ = sub;
        if (downstream_) {
            downstream_->onSubscribe(this);
        }
    }

    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (cancelled_ || !downstream_) return;

        try {
            R mapped = mapper_(value);
            downstream_->onNext(mapped);
        } catch (const std::exception& ex) {
            cancelled_ = true;
            if (upstream_) upstream_->cancel();
            downstream_->onError(ex);
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

template <typename T, typename R>
class TransformPublisher : public Publisher<R> {
private:
    std::shared_ptr<Publisher<T>> source_;
    std::function<R(const T&)> mapper_;
    std::mutex mtx_;
    std::vector<std::shared_ptr<TransformSubscription<T, R>>> active_subs_;

public:
    TransformPublisher(std::shared_ptr<Publisher<T>> source, std::function<R(const T&)> mapper)
        : source_(std::move(source)), mapper_(std::move(mapper)) {}

    void subscribe(Subscriber<R>* subscriber) override {
        if (!subscriber || !source_) return;
        auto sub = std::make_shared<TransformSubscription<T, R>>(subscriber, mapper_);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            active_subs_.push_back(sub);
        }
        source_->subscribe(sub.get());
    }
};

// ==============================================================================
// 2. Filter Operator: Lọc các phần tử thỏa mãn điều kiện
// ==============================================================================

template <typename T>
class FilterSubscription : public Subscriber<T>,
                           public Subscription,
                           public std::enable_shared_from_this<FilterSubscription<T>> {
private:
    Subscriber<T>* downstream_;
    std::function<bool(const T&)> predicate_;
    Subscription* upstream_{nullptr};
    std::mutex mtx_;
    bool cancelled_{false};

public:
    FilterSubscription(Subscriber<T>* downstream, std::function<bool(const T&)> predicate)
        : downstream_(downstream), predicate_(std::move(predicate)) {}

    void onSubscribe(Subscription* sub) override {
        upstream_ = sub;
        if (downstream_) {
            downstream_->onSubscribe(this);
        }
    }

    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (cancelled_ || !downstream_) return;

        try {
            if (predicate_(value)) {
                downstream_->onNext(value);
            } else {
                // Bù đắp demand: khi một item bị loại bỏ, yêu cầu upstream thêm 1 phần tử
                // để không làm thiếu hụt số lượng downstream đã yêu cầu qua request(n)
                if (upstream_) {
                    upstream_->request(1);
                }
            }
        } catch (const std::exception& ex) {
            cancelled_ = true;
            if (upstream_) upstream_->cancel();
            downstream_->onError(ex);
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
class FilterPublisher : public Publisher<T> {
private:
    std::shared_ptr<Publisher<T>> source_;
    std::function<bool(const T&)> predicate_;
    std::mutex mtx_;
    std::vector<std::shared_ptr<FilterSubscription<T>>> active_subs_;

public:
    FilterPublisher(std::shared_ptr<Publisher<T>> source, std::function<bool(const T&)> predicate)
        : source_(std::move(source)), predicate_(std::move(predicate)) {}

    void subscribe(Subscriber<T>* subscriber) override {
        if (!subscriber || !source_) return;
        auto sub = std::make_shared<FilterSubscription<T>>(subscriber, predicate_);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            active_subs_.push_back(sub);
        }
        source_->subscribe(sub.get());
    }
};

// ==============================================================================
// 3. FlatMap Operator: Làm phẳng T -> std::vector<R>
// ==============================================================================

template <typename T, typename R>
class FlatMapSubscription : public Subscriber<T>,
                            public Subscription,
                            public std::enable_shared_from_this<FlatMapSubscription<T, R>> {
private:
    Subscriber<R>* downstream_;
    std::function<std::vector<R>(const T&)> flattener_;
    Subscription* upstream_{nullptr};
    std::mutex mtx_;
    bool cancelled_{false};

public:
    FlatMapSubscription(Subscriber<R>* downstream, std::function<std::vector<R>(const T&)> flattener)
        : downstream_(downstream), flattener_(std::move(flattener)) {}

    void onSubscribe(Subscription* sub) override {
        upstream_ = sub;
        if (downstream_) {
            downstream_->onSubscribe(this);
        }
    }

    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (cancelled_ || !downstream_) return;

        try {
            std::vector<R> items = flattener_(value);
            for (const auto& item : items) {
                if (cancelled_) break;
                downstream_->onNext(item);
            }
        } catch (const std::exception& ex) {
            cancelled_ = true;
            if (upstream_) upstream_->cancel();
            downstream_->onError(ex);
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

template <typename T, typename R>
class FlatMapPublisher : public Publisher<R> {
private:
    std::shared_ptr<Publisher<T>> source_;
    std::function<std::vector<R>(const T&)> flattener_;
    std::mutex mtx_;
    std::vector<std::shared_ptr<FlatMapSubscription<T, R>>> active_subs_;

public:
    FlatMapPublisher(std::shared_ptr<Publisher<T>> source, std::function<std::vector<R>(const T&)> flattener)
        : source_(std::move(source)), flattener_(std::move(flattener)) {}

    void subscribe(Subscriber<R>* subscriber) override {
        if (!subscriber || !source_) return;
        auto sub = std::make_shared<FlatMapSubscription<T, R>>(subscriber, flattener_);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            active_subs_.push_back(sub);
        }
        source_->subscribe(sub.get());
    }
};

// ==============================================================================
// 4. LambdaSubscriber: Kết nối cuối pipeline chuyển dữ liệu sang IDataSink<T>
// ==============================================================================

template <typename T>
class LambdaSubscriber : public Subscriber<T>,
                         public std::enable_shared_from_this<LambdaSubscriber<T>> {
private:
    std::shared_ptr<IDataSink<T>> sink_;
    std::shared_ptr<Publisher<T>> pipelineAnchor_;
    Subscription* sub_{nullptr};
    std::shared_ptr<LambdaSubscriber<T>> self_;
    std::mutex mtx_;
    bool terminated_{false};

public:
    LambdaSubscriber(std::shared_ptr<IDataSink<T>> sink,
                     std::shared_ptr<Publisher<T>> pipelineAnchor)
        : sink_(std::move(sink)), pipelineAnchor_(std::move(pipelineAnchor)) {}

    void start() {
        self_ = this->shared_from_this();
    }

    void onSubscribe(Subscription* sub) override {
        sub_ = sub;
        if (sub_) {
            // Yêu cầu dữ liệu không giới hạn (unbounded demand) cho terminal sink
            sub_->request(std::numeric_limits<int>::max());
        }
    }

    void onNext(const T& value) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (terminated_ || !sink_) return;
        sink_->onNext(value);
    }

    void onError(const std::exception& ex) override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (terminated_) return;
        terminated_ = true;
        if (sink_) {
            sink_->onError(ex);
        }
        self_.reset(); // Giải phóng vòng giữ bản thân
    }

    void onComplete() override {
        std::lock_guard<std::mutex> lock(mtx_);
        if (terminated_) return;
        terminated_ = true;
        if (sink_) {
            sink_->onComplete();
        }
        self_.reset(); // Giải phóng vòng giữ bản thân
    }
};

} // namespace detail

// ==============================================================================
// Hiện thực các phương thức của lớp DataStream<T>
// ==============================================================================

template <typename T>
inline DataStream<T>::DataStream(std::shared_ptr<Publisher<T>> source)
    : source_(std::move(source)) {}

template <typename T>
template <typename R>
inline DataStream<R> DataStream<T>::map(std::function<R(const T&)> mapper) {
    auto mapPub = std::make_shared<detail::TransformPublisher<T, R>>(source_, std::move(mapper));
    return DataStream<R>(mapPub);
}

template <typename T>
inline DataStream<T> DataStream<T>::filter(std::function<bool(const T&)> predicate) {
    auto filterPub = std::make_shared<detail::FilterPublisher<T>>(source_, std::move(predicate));
    return DataStream<T>(filterPub);
}

template <typename T>
template <typename R>
inline DataStream<R> DataStream<T>::flatMap(std::function<std::vector<R>(const T&)> flattener) {
    auto flatMapPub = std::make_shared<detail::FlatMapPublisher<T, R>>(source_, std::move(flattener));
    return DataStream<R>(flatMapPub);
}

template <typename T>
inline void DataStream<T>::to(std::shared_ptr<IDataSink<T>> sink) {
    if (!sink || !source_) return;
    auto lambdaSub = std::make_shared<detail::LambdaSubscriber<T>>(sink, source_);
    lambdaSub->start();
    this->subscribe(lambdaSub);
}

template <typename T>
inline std::shared_ptr<Publisher<T>> DataStream<T>::getSource() const {
    return source_;
}

template <typename T>
inline void DataStream<T>::subscribe(std::shared_ptr<Subscriber<T>> sub) {
    if (source_ && sub) {
        source_->subscribe(sub.get());
    }
}

template <typename T>
inline void DataStream<T>::subscribe(Subscriber<T>* sub) {
    if (source_ && sub) {
        source_->subscribe(sub);
    }
}

} // namespace iot_stream

