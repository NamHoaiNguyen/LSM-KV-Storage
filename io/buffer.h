#ifndef IO_BUFFER_H
#define IO_BUFFER_H

#include "common/macros.h"

#include <span>

namespace kvs {

class Buffer {
public:
  static constexpr size_t kDefaultCapacity = 4 * 1024; // 4KB

  explicit Buffer(size_t capacity = kDefaultCapacity);

  ~Buffer() = default;

  // No copy allowed
  Buffer(const Buffer &) = delete;
  Buffer &operator=(Buffer &) = delete;

  // Move constructor/assignment
  Buffer(Buffer &&) = default;
  Buffer &operator=(Buffer &&) = default;

  std::span<Byte> GetBuffer();

  std::span<const Byte> GetImmutableBuffer() const;

  size_t GetBufferLength() const;

private:
  DynamicBuffer buffer_;
};

} // namespace kvs

#endif // IO_BUFFER_H