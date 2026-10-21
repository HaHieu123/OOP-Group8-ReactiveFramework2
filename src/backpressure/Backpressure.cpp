#include "backpressure/Backpressure.h"
#include "source/Sensor.h"
#include <cstdint>
#include <string>

namespace iot_stream {

// ==============================================================================
// Explicit Template Instantiations cho thư viện tĩnh iot_stream
// ==============================================================================

template class BackpressureSubscription<int>;
template class BackpressureSubscription<double>;
template class BackpressureSubscription<float>;
template class BackpressureSubscription<int64_t>;
template class BackpressureSubscription<std::string>;
template class BackpressureSubscription<SensorReading>;

namespace detail {

/**
 * @brief Helper trả về thông tin module Backpressure.
 */
const char* get_backpressure_module_info() noexcept {
    return "iot_stream_framework::backpressure (BUFFER, DROP, LATEST, ERROR strategies)";
}

} // namespace detail

} // namespace iot_stream

