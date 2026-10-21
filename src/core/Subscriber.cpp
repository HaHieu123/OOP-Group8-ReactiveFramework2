#include "core/LambdaSubscriber.h"
#include <cstdint>
#include <string>

namespace iot_stream {

// ==============================================================================
// Explicit Template Instantiations
// Đảm bảo LambdaSubscriber được biên dịch sẵn vào iot_stream static library
// ==============================================================================

template class LambdaSubscriber<int>;
template class LambdaSubscriber<double>;
template class LambdaSubscriber<float>;
template class LambdaSubscriber<int64_t>;
template class LambdaSubscriber<std::string>;

} // namespace iot_stream

