#include "sinks/ConsoleSink.h"
#include <string>

namespace iot_stream {

// ==============================================================================
// Explicit Template Instantiations cho thư viện tĩnh iot_stream
// ==============================================================================

template class ConsoleSink<int>;
template class ConsoleSink<double>;
template class ConsoleSink<float>;
template class ConsoleSink<std::string>;
template class ConsoleSink<SensorReading>;

namespace detail {

/**
 * @brief Helper trả về thông tin module ConsoleSink.
 */
const char* get_console_sink_module_info() noexcept {
    return "iot_stream_framework::sinks::ConsoleSink";
}

} // namespace detail

} // namespace iot_stream

