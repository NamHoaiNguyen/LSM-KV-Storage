#ifndef COMMON_MACROS
#define COMMON_MACROS

#include <cstdint>

namespace kvs {

using Fd = int; // FileDescriptor

using TxnId = uint64_t; // Transaction Id

using TimeStamp = uint64_t; // Transaction timestamp

} // namespace kvs

#endif // COMMON_MACROS