#pragma once

#include <memory>

namespace iot_stream {

// Khai báo trước (forward declaration) cho lớp DataStream
template <typename T>
class DataStream;

/**
 * @brief Giao diện đánh dấu (Marker Interface / Base Interface) cho các kiểu Stream.
 * 
 * Mục đích:
 * - Dùng để hỗ trợ tính đa hình (Polymorphism) và lưu trữ tổng quát nếu cần
 *   (ví dụ: quản lý danh sách các stream không đồng nhất hoặc đóng gói trừu tượng).
 * - Cung cấp virtual destructor mặc định để bảo đảm giải phóng an toàn qua con trỏ interface.
 * 
 * @tparam T Kiểu dữ liệu của các phần tử luân chuyển trong Stream.
 */
template <typename T>
class IStream {
public:
    virtual ~IStream() = default;
};

} // namespace iot_stream

