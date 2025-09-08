#include "sstable/block_index.h"

namespace kvs {

namespace sstable {

BlockIndex::BlockIndex(std::string_view smallest_key,
                       std::string_view largest_key,
                       uint64_t block_start_offset, uint64_t block_size)
    : smallest_key_(smallest_key), largest_key_(largest_key),
      block_start_offset_(block_start_offset), block_size_(block_size) {}

std::string_view BlockIndex::GetSmallestKey() const { return smallest_key_; }

std::string_view BlockIndex::GetLargestKey() const { return largest_key_; }

uint64_t BlockIndex::GetBlockStartOffset() const { return block_start_offset_; }

uint64_t BlockIndex::GetBlockSize() const { return block_size_; }

} // namespace sstable

} // namespace kvs