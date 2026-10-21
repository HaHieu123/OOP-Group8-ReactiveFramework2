#include "operators/Window.h"
#include <string>

namespace iot_stream {

// ==============================================================================
// Explicit Template Instantiations cho thư viện tĩnh iot_stream
// ==============================================================================

template class Window<int>;
template class Window<double>;
template class Window<std::string>;

template class TumblingWindow<int>;
template class TumblingWindow<double>;
template class TumblingWindow<std::string>;

template class SlidingWindow<int>;
template class SlidingWindow<double>;
template class SlidingWindow<std::string>;

template class CountWindow<int>;
template class CountWindow<double>;
template class CountWindow<std::string>;

namespace detail {

/**
 * @brief Helper trả về thông tin module Windowing.
 */
const char* get_windowing_module_info() noexcept {
    return "iot_stream_framework::operators::Window (Tumbling, Sliding, Count Windowing)";
}

} // namespace detail

} // namespace iot_stream

