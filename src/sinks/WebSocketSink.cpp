#include "sinks/WebSocketSink.h"
#include "source/Sensor.h"
#include <string>

namespace iot_stream {

// ==============================================================================
// Explicit Template Instantiations cho thư viện tĩnh iot_stream
// ==============================================================================

template class WebSocketSink<int>;
template class WebSocketSink<double>;
template class WebSocketSink<float>;
template class WebSocketSink<std::string>;
template class WebSocketSink<SensorReading>;

namespace detail {

/**
 * @brief Helper trả về thông tin module WebSocketSink.
 */
const char* get_websocket_sink_module_info() noexcept {
    return "iot_stream_framework::sinks::WebSocketSink (STUB via Callback)";
}

} // namespace detail

} // namespace iot_stream

