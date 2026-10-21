#pragma once

#include "core/IStream.h"
#include "core/Publisher.h"
#include "core/Subscriber.h"
#include "core/Subscription.h"
#include "sinks/IDataSink.h"

#include <functional>
#include <memory>
#include <type_traits>
#include <vector>

namespace iot_stream {

// Forward declaration cho Windowing module
template <typename T>
class Window;

template <typename T>
class IWindowStrategy;

/**
 * @brief Lớp trung tâm của framework, cung cấp Fluent Interface cho Stream Processing Pipeline.
 * 
 * DataStream đại diện cho một luồng dữ liệu phản ứng (Reactive Stream) bất biến (immutable pipeline),
 * cho phép xâu chuỗi các toán tử xử lý như map, filter, flatMap, window và chuyển dữ liệu đến Sink.
 * 
 * Luồng hoạt động (Dataflow):
 *   Source (Publisher) -> filter(...) -> map(...) -> flatMap(...) -> window(...) -> to(Sink)
 * 
 * @tparam T Kiểu dữ liệu của các phần tử trong luồng hiện tại.
 */
template <typename T>
class DataStream : public IStream<T> {
public:
    /**
     * @brief Khởi tạo một DataStream từ một nguồn phát (Publisher).
     * @param source Con trỏ shared_ptr tới nguồn phát dữ liệu.
     */
    explicit DataStream(std::shared_ptr<Publisher<T>> source);

    /**
     * @brief Toán tử biến đổi từng phần tử trong luồng sang kiểu dữ liệu mới R.
     * @tparam R Kiểu dữ liệu đầu ra sau khi biến đổi.
     * @param mapper Hàm chuyển đổi T -> R.
     * @return DataStream<R> Luồng dữ liệu mới mang kiểu R.
     */
    template <typename R>
    DataStream<R> map(std::function<R(const T&)> mapper);

    /**
     * @brief Overload của map hỗ trợ lambda tự động suy diễn kiểu trả về (type deduction).
     */
    template <typename F, typename R = std::invoke_result_t<F, const T&>>
    DataStream<R> map(F mapper) {
        return map<R>(std::function<R(const T&)>(mapper));
    }

    /**
     * @brief Toán tử lọc: chỉ cho phép các phần tử thỏa mãn điều kiện đi tiếp.
     * @param predicate Hàm kiểm tra điều kiện (trả về true để giữ lại, false để loại bỏ).
     * @return DataStream<T> Luồng dữ liệu sau khi lọc.
     */
    DataStream<T> filter(std::function<bool(const T&)> predicate);

    /**
     * @brief Toán tử làm phẳng: biến mỗi phần tử T thành một tập hợp std::vector<R>
     *        và phát lần lượt từng phần tử R vào luồng mới.
     * @tparam R Kiểu dữ liệu của các phần tử sau khi làm phẳng.
     * @param flattener Hàm chuyển đổi T -> std::vector<R>.
     * @return DataStream<R> Luồng dữ liệu mới.
     */
    template <typename R>
    DataStream<R> flatMap(std::function<std::vector<R>(const T&)> flattener);

    /**
     * @brief Overload của flatMap hỗ trợ lambda tự động suy diễn kiểu vector.
     */
    template <typename F, typename R = typename std::invoke_result_t<F, const T&>::value_type>
    DataStream<R> flatMap(F flattener) {
        return flatMap<R>(std::function<std::vector<R>(const T&)>(flattener));
    }

    /**
     * @brief Toán tử gom nhóm cửa sổ dữ liệu (Windowing).
     * Forward declaration: template method được định nghĩa chi tiết trong operators/Window.h.
     * 
     * @tparam Strategy Kiểu chiến lược cửa sổ (kế thừa từ IWindowStrategy<T>).
     * @param windowStrategy Con trỏ shared_ptr tới chiến lược cửa sổ.
     * @return DataStream<Window<T>> Luồng các cửa sổ dữ liệu Window<T>.
     */
    template <typename Strategy>
    DataStream<Window<T>> window(std::shared_ptr<Strategy> windowStrategy);

    /**
     * @brief Điểm kết thúc của Pipeline: chuyển toàn bộ dữ liệu luồng tới một Sink.
     * Kết nối một LambdaSubscriber vào cuối luồng để nhận dữ liệu liên tục.
     * 
     * @param sink Con trỏ shared_ptr tới đích nhận dữ liệu (IDataSink<T>).
     */
    void to(std::shared_ptr<IDataSink<T>> sink);

    /**
     * @brief Lấy nguồn phát (Publisher) gốc hiện tại của Stream.
     */
    std::shared_ptr<Publisher<T>> getSource() const;

    /**
     * @brief Cầu nối (bridge) cho phép đăng ký trực tiếp một Subscriber vào luồng.
     * @param sub Con trỏ shared_ptr tới Subscriber.
     */
    void subscribe(std::shared_ptr<Subscriber<T>> sub);

    /**
     * @brief Cầu nối hỗ trợ con trỏ thô Subscriber<T>*.
     */
    void subscribe(Subscriber<T>* sub);

private:
    std::shared_ptr<Publisher<T>> source_;
};

} // namespace iot_stream

// Nạp phần hiện thực chi tiết của template DataStream
#include "core/DataStream.tpp"

/*
================================================================================
Ví dụ sử dụng Fluent API:
================================================================================

    auto source = std::make_shared<SensorPublisher>();
    auto consoleSink = std::make_shared<ConsoleSink<std::string>>();

    auto stream = DataStream<int>(source);
    stream.filter([](int x) { return x % 2 == 0; })
          .map([](int x) { return std::to_string(x); })
          .to(consoleSink);

================================================================================
*/

