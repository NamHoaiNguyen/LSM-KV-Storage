#include "io/buffer.h"

namespace kvs {

namespace io {

Buffer::Buffer(size_t capacity) { buffer_.reserve(capacity); }

std::span<Byte> Buffer::GetBufferView() { return std::span<Byte>(buffer_); }

size_t Buffer::GetBufferLength() const { return buffer_.size(); }

std::span<const Byte> Buffer::GetImmutableBufferView() const {
  return std::span<const Byte>(buffer_);
}

DynamicBuffer &Buffer::GetBuffer() { return buffer_; }

void Buffer::WriteData(DynamicBuffer &&buffer) { buffer_ = std::move(buffer); }

} // namespace io

} // namespace kvs