#ifndef IO_BUFFER_H
#define IO_BUFFER_H

#include "common/macros.h"

#include <span>

namespace kvs {

class Buffer {
public:
  explicit Buffer(size_t capacity = kDefaultBufferSize);

  ~Buffer() = default;

  // No copy allowed
  Buffer(const Buffer &) = delete;
  Buffer &operator=(Buffer &) = delete;

  // Move constructor/assignment
  Buffer(Buffer &&) = default;
  Buffer &operator=(Buffer &&) = default;

  std::span<Byte> GetBufferView();

  std::span<const Byte> GetImmutableBufferView() const;

  DynamicBuffer &GetBuffer();

  size_t GetBufferLength() const;

  void WriteData(DynamicBuffer &&buffer);

private:
  DynamicBuffer buffer_;
};

} // namespace kvs

#endif // IO_BUFFER_H