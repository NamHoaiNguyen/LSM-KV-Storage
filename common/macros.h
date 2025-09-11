#ifndef COMMON_MACROS
#define COMMON_MACROS

#include <cstdint>
#include <vector>

namespace kvs {

using Fd = int; // FileDescriptor

using SSTId = uint64_t;

using TxnId = uint64_t; // Transaction Id

using TimeStamp = uint64_t; // Transaction timestamp

using DynamicBuffer = std::vector<uint8_t>;

using Byte = uint8_t;

using DoubleByte = uint16_t;

constexpr size_t kDefaultBufferSize = 8 * 1024; // 8 KB

constexpr size_t kMaxKeySize = 4 * 1024; // 4 KB

constexpr TxnId INVALID_TXN_ID = -1;

} // namespace kvs

#endif // COMMON_MACROS