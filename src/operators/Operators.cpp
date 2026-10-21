#include "operators/Operators.h"
#include <string>

namespace iot_stream {

// ==============================================================================
// Explicit Template Instantiations cho thư viện tĩnh iot_stream
// ==============================================================================

template class MapOperation<int, std::string>;
template class MapOperation<double, std::string>;
template class MapOperation<int, double>;

template class FilterOperation<int>;
template class FilterOperation<double>;
template class FilterOperation<std::string>;

template class FlatMapOperation<int, int>;
template class FlatMapOperation<std::string, std::string>;

namespace detail {

/**
 * @brief Helper trả về thông tin module Operators.
 */
const char* get_operators_module_info() noexcept {
    return "iot_stream_framework::operators (Strategy Pattern Operations)";
}

} // namespace detail

} // namespace iot_stream

