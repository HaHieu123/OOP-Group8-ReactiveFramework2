#include "sinks/FileSink.h"
#include <string>

namespace iot_stream {

// ==============================================================================
// Explicit Template Instantiations cho thư viện tĩnh iot_stream
// ==============================================================================

template class FileSink<int>;
template class FileSink<double>;
template class FileSink<float>;
template class FileSink<std::string>;
template class FileSink<SensorReading>;

namespace detail {

/**
 * @brief Helper trả về thông tin module FileSink.
 */
const char* get_file_sink_module_info() noexcept {
    return "iot_stream_framework::sinks::FileSink";
}

} // namespace detail

} // namespace iot_stream

