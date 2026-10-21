#pragma once

#include <memory>

namespace iot_stream {

/**
 * @brief Giao diện đại diện cho mối liên kết (hợp đồng đăng ký) giữa Subscriber và Publisher.
 * 
 * Contract:
 * - Sau khi Subscriber gọi Publisher::subscribe(), Publisher sẽ tạo một Subscription
 *   và chuyển cho Subscriber thông qua Subscriber::onSubscribe(Subscription*).
 * - Subscriber nắm giữ Subscription để kiểm soát luồng dữ liệu (Flow Control / Backpressure)
 *   thông qua hàm request(n) và có thể hủy nhận bất kỳ lúc nào bằng cancel().
 */
class Subscription {
public:
    virtual ~Subscription() = default;

    /**
     * @brief Subscriber yêu cầu Publisher phát thêm n phần tử dữ liệu (Backpressure demand).
     * 
     * Đây là cơ chế điều phối áp lực ngược:
     * - n > 0: Subscriber báo cho Publisher biết mình sẵn sàng xử lý thêm tối đa n phần tử.
     *          Publisher chỉ được phép gọi onNext tối đa n lần cho đến khi nhận được request tiếp theo.
     * - n <= 0: Yêu cầu không hợp lệ (có thể kích hoạt onError hoặc ném ngoại lệ).
     * 
     * @param n Số lượng phần tử yêu cầu phát tiếp.
     */
    virtual void request(int n) = 0;

    /**
     * @brief Subscriber chủ động hủy việc đăng ký nhận dữ liệu từ Publisher.
     * 
     * Sau khi gọi cancel():
     * - Publisher phải dừng gửi dữ liệu (onNext), lỗi (onError) hay hoàn tất (onComplete).
     * - Các tài nguyên liên quan tới đăng ký này sẽ được giải phóng an toàn.
     */
    virtual void cancel() = 0;
};

} // namespace iot_stream

