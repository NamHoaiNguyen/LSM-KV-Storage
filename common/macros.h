#ifndef COMMON_MACROS
#define COMMON_MACROS

#include <cstdint>
#include <vector>

namespace kvs {

using Fd = int; // FileDescriptor

using TxnId = uint64_t; // Transaction Id

using TimeStamp = uint64_t; // Transaction timestamp

using DynamicBuffer = std::vector<uint8_t>;

using Byte = uint8_t;

} // namespace kvs

#endif // COMMON_MACROS