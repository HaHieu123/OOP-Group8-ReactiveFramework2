#pragma once

#include <exception>

namespace iot_stream {

/**
 * @brief Giao diện đích đến của dữ liệu (Data Sink) tại điểm kết thúc của Pipeline.
 * 
 * DataSink đại diện cho các đầu ra như Console, File CSV, WebSocket, Memory Queue...
 * 
 * Contract:
 * - onNext: Được gọi khi có phần tử dữ liệu mới đi qua hết các toán tử trong pipeline.
 * - onError: Được gọi khi có ngoại lệ phát sinh trong pipeline.
 * - onComplete: Được gọi khi luồng dữ liệu kết thúc thành công.
 * 
 * @tparam T Kiểu dữ liệu của các phần tử luồng.
 */
template <typename T>
class IDataSink {
public:
    virtual ~IDataSink() = default;

    virtual void onNext(const T& value) = 0;
    virtual void onError(const std::exception& ex) = 0;
    virtual void onComplete() = 0;
    virtual void flush() {}

    // Aliases phong cách snake_case tiện lợi
    virtual void on_data(const T& value) { onNext(value); }
    virtual void on_error(const std::exception& ex) { onError(ex); }
    virtual void on_complete() { onComplete(); }
};

} // namespace iot_stream

