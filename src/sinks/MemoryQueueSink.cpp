#include "sinks/MemoryQueueSink.h"
#include "source/Sensor.h"
#include <string>

namespace iot_stream {

// ==============================================================================
// Explicit Template Instantiations cho thư viện tĩnh iot_stream
// ==============================================================================

template class MemoryQueueSink<int>;
template class MemoryQueueSink<double>;
template class MemoryQueueSink<float>;
template class MemoryQueueSink<std::string>;
template class MemoryQueueSink<SensorReading>;

namespace detail {

/**
 * @brief Helper trả về thông tin module MemoryQueueSink.
 */
const char* get_memory_queue_sink_module_info() noexcept {
    return "iot_stream_framework::sinks::MemoryQueueSink";
}

} // namespace detail

} // namespace iot_stream

