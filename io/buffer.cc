#include "io/buffer.h"

namespace kvs {

Buffer::Buffer(size_t capacity) { buffer_.reserve(capacity); }

std::span<Byte> Buffer::GetBuffer() { return std::span<Byte>(buffer_); }

size_t Buffer::GetBufferLength() const { return buffer_.size(); }

std::span<const Byte> Buffer::GetImmutableBuffer() const {
  return std::span<const Byte>(buffer_);
}

} // namespace kvs