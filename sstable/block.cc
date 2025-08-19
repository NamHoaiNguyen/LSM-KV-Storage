#include "sstable/block.h"

namespace kvs {

Block::Block() = default;

Block::Block(std::span<const uint8_t> data) : data_(data) {}

void Block::Decode() {}

void Block::Encode() {}

} // namespace kvs