#pragma once

#include "core/Subscriber.h"
#include <memory>

namespace iot_stream {

/**
 * @brief Giao diện nguồn phát dữ liệu (Provider / Observable) theo chuẩn Reactive Streams.
 * 
 * Contract:
 * - Publisher là nguồn phát sinh các phần tử dữ liệu có kiểu T.
 * - Khi một Subscriber gọi subscribe(subscriber), Publisher có trách nhiệm:
 *   1. Tạo một đối tượng Subscription hợp lệ.
 *   2. Gọi subscriber->onSubscribe(subscription) để thiết lập kênh giao tiếp hai chiều.
 * - Publisher chỉ được phép gửi dữ liệu tới Subscriber khi Subscriber đã gửi tín hiệu
 *   yêu cầu qua subscription->request(n).
 * 
 * @tparam T Kiểu dữ liệu của các phần tử mà Publisher phát ra.
 */
template <typename T>
class Publisher {
public:
    virtual ~Publisher() = default;

    /**
     * @brief Đăng ký một Subscriber mới để lắng nghe luồng dữ liệu.
     * 
     * @param subscriber Con trỏ tới đối tượng Subscriber muốn đăng ký nhận dữ liệu.
     *                   Nếu subscriber == nullptr, hành vi có thể bỏ qua hoặc ném ngoại lệ.
     */
    virtual void subscribe(Subscriber<T>* subscriber) = 0;
};

} // namespace iot_stream

