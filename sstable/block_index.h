#ifndef SSTABLE_BLOCK_INDEX_H
#define SSTABLE_BLOCK_INDEX_H

#include <common/macros.h>

// libC++
#include <span>
#include <string>

namespace kvs {

/*
Block meta format
-----------------------------------------------------------------------------
| 1st_key_len(4B) | 1st_key | last_key_len(4B) | last_key |                 |
| starting offset of corresponding data block (8B) | size of data block(8B) |
-----------------------------------------------------------------------------
*/

namespace sstable {

class BlockIndex {
public:
  BlockIndex() = default;
  BlockIndex(std::string_view smallest_key, std::string_view largest_key,
             uint64_t block_start_offset, uint64_t block_size);

  ~BlockIndex() = default;

  // No copy allowed
  BlockIndex(const BlockIndex &) = delete;
  BlockIndex &operator=(BlockIndex &) = delete;

  // Move constructor/assignment
  BlockIndex(BlockIndex &&) = default;
  BlockIndex &operator=(BlockIndex &&) = default;

  // NOT best practise. But it is ok
  std::string_view GetSmallestKey() const;

  std::string_view GetLargestKey() const;

  const uint64_t GetBlockStartOffset() const;

  const uint64_t GetBlockSize() const;

private:
  std::vector<Byte> buffer_;

  const std::string smallest_key_;

  const std::string largest_key_;

  const uint64_t block_start_offset_;

  const uint64_t block_size_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_INDEX_H