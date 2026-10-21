#pragma once

namespace iot_stream {

template <typename T>
void BackpressureSubscription<T>::request(int n) {
    if (n <= 0) return;

    demand_ += n;

    // Xả (flush) các phần tử đang chờ trong buffer / latest slot
    while (true) {
        T item;
        bool has_item = false;
        bool should_complete = false;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (cancelled_) return;

            if (demand_ > 0) {
                if (strategy_ == BackpressureStrategy::BUFFER && !buffer_.empty()) {
                    item = std::move(buffer_.front());
                    buffer_.pop_front();
                    demand_--;
                    has_item = true;
                } else if (strategy_ == BackpressureStrategy::LATEST && latest_.has_value()) {
                    item = std::move(*latest_);
                    latest_.reset();
                    demand_--;
                    has_item = true;
                }
            }

            if (!has_item) {
                if (completed_ && buffer_.empty() && !latest_.has_value()) {
                    should_complete = true;
                }
                break;
            }
        }

        if (has_item && downstream_) {
            try {
                downstream_->onNext(item);
            } catch (const std::exception& ex) {
                cancel();
                downstream_->onError(ex);
                return;
            }
        }

        if (should_complete && downstream_) {
            downstream_->onComplete();
            break;
        }
    }

    cv_.notify_all();
}

template <typename T>
void BackpressureSubscription<T>::cancel() {
    std::lock_guard<std::mutex> lock(mtx_);
    cancelled_ = true;
    buffer_.clear();
    latest_.reset();
    cv_.notify_all();
}

template <typename T>
void BackpressureSubscription<T>::onNext(const T& value) {
    T item_to_dispatch;
    bool dispatch_direct = false;
    bool emit_error = false;
    std::string error_msg;

    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (cancelled_ || completed_) return;

        // Nếu có demand và không có phần tử nào đang xếp hàng tồn đọng:
        if (demand_ > 0 && buffer_.empty() && !latest_.has_value()) {
            demand_--;
            item_to_dispatch = value;
            dispatch_direct = true;
        } else {
            // Trường hợp demand == 0 hoặc đang có hàng đợi cần bảo toàn thứ tự:
            switch (strategy_) {
                case BackpressureStrategy::BUFFER: {
                    if (buffer_.size() < capacity_) {
                        buffer_.push_back(value);
                    } else {
                        // Buffer đầy: Bỏ qua và tăng số lượng dropped
                        droppedCount_++;
                    }
                    break;
                }
                case BackpressureStrategy::DROP: {
                    // Loại bỏ ngay lập tức
                    droppedCount_++;
                    break;
                }
                case BackpressureStrategy::LATEST: {
                    if (latest_.has_value()) {
                        droppedCount_++; // Ghi đè phần tử cũ chưa kịp tiêu thụ
                    }
                    latest_ = value;
                    break;
                }
                case BackpressureStrategy::ERROR: {
                    cancelled_ = true;
                    emit_error = true;
                    error_msg = "Backpressure overflow under ERROR strategy (capacity=" +
                                std::to_string(capacity_) + ", demand=0).";
                    break;
                }
            }
        }
    }

    if (dispatch_direct && downstream_) {
        try {
            downstream_->onNext(item_to_dispatch);
        } catch (const std::exception& ex) {
            cancel();
            downstream_->onError(ex);
        }
    } else if (emit_error && downstream_) {
        downstream_->onError(BackpressureOverflowException(error_msg));
    }
}

template <typename T>
void BackpressureSubscription<T>::onComplete() {
    bool call_complete_now = false;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (cancelled_ || completed_) return;
        completed_ = true;
        if (buffer_.empty() && !latest_.has_value()) {
            call_complete_now = true;
        }
    }
    if (call_complete_now && downstream_) {
        downstream_->onComplete();
    }
}

template <typename T>
void BackpressureSubscription<T>::onError(const std::exception& ex) {
    bool call_error = false;
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (cancelled_ || completed_) return;
        cancelled_ = true;
        call_error = true;
    }
    if (call_error && downstream_) {
        downstream_->onError(ex);
    }
}

} // namespace iot_stream

