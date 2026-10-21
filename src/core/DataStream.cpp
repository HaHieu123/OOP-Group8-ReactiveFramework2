#include "core/DataStream.h"

namespace iot_stream {

// ==============================================================================
// Các non-template helper và runtime diagnostics cho DataStream
// ==============================================================================

namespace detail {

/**
 * @brief Trả về thông tin phiên bản của lõi Streaming Engine.
 */
const char* get_stream_framework_version() noexcept {
    return "iot_stream_framework v1.0.0 (C++17 Reactive Pipeline)";
}

} // namespace detail

} // namespace iot_stream

