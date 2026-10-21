#pragma once

#include "core/Subscription.h"
#include <memory>
#include <stdexcept>

namespace iot_stream {

/**
 * @brief Giao diện nhận dữ liệu (Consumer / Observer) theo chuẩn Reactive Streams.
 * 
 * Vòng đời sự kiện (Reactive Contract):
 *   onSubscribe -> onNext* -> (onError | onComplete)
 * 
 * Quy tắc:
 * 1. onSubscribe() luôn được gọi đầu tiên khi Subscriber đăng ký thành công với Publisher.
 * 2. onNext() được gọi tuần tự 0 hoặc nhiều lần khi có dữ liệu mới, với điều kiện tổng số
 *    lần gọi không vượt quá số lượng n mà Subscriber đã yêu cầu qua Subscription::request(n).
 * 3. onError() hoặc onComplete() là trạng thái kết thúc (Terminal State). Sau khi một trong hai
 *    hàm này được gọi, Subscription xem như kết thúc và không có thêm bất kỳ sự kiện nào nữa.
 * 
 * @tparam T Kiểu dữ liệu của các phần tử trong luồng (Stream Event).
 */
template <typename T>
class Subscriber {
public:
    virtual ~Subscriber() = default;

    /**
     * @brief Được gọi một lần duy nhất khi Publisher chấp nhận việc đăng ký.
     * 
     * Subscriber nhận con trỏ Subscription để quản lý vòng đời và gửi yêu cầu
     * số lượng phần tử cần nhận qua sub->request(n).
     * 
     * @param sub Con trỏ quản lý subscription giữa Publisher và Subscriber.
     */
    virtual void onSubscribe(Subscription* sub) = 0;

    /**
     * @brief Nhận phần tử dữ liệu tiếp theo từ luồng.
     * 
     * @param value Giá trị dữ liệu mới được phát ra từ pipeline.
     */
    virtual void onNext(const T& value) = 0;

    /**
     * @brief Được gọi khi có lỗi nghiêm trọng xảy ra trong quá trình xử lý luồng.
     * 
     * Đây là sự kiện kết thúc (Terminal Event). Sau khi gọi onError, Publisher sẽ
     * dừng phát dữ liệu và giải phóng subscription.
     * 
     * @param ex Ngoại lệ mô tả chi tiết lỗi phát sinh.
     */
    virtual void onError(const std::exception& ex) = 0;

    /**
     * @brief Được gọi khi Publisher đã hoàn thành việc phát tất cả dữ liệu hợp lệ.
     * 
     * Đây là sự kiện kết thúc thành công (Terminal Event). Sau khi gọi onComplete,
     * luồng được đóng lại.
     */
    virtual void onComplete() = 0;
};

} // namespace iot_stream

