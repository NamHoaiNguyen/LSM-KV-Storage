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

  ~BlockIndex() = default;

  // No copy allowed
  BlockIndex(const BlockIndex &) = delete;
  BlockIndex &operator=(BlockIndex &) = delete;

  // Move constructor/assignment
  BlockIndex(BlockIndex &&) = default;
  BlockIndex &operator=(BlockIndex &&) = default;

  void AddEntry(std::string_view first_key, std::string_view last_key,
                uint64_t block_start_offset, uint64_t block_length);

  // ONLY call this method after finish writing all data to block.
  // Otherwise, it can cause dangling pointer.
  std::span<const Byte> GetBufferView();

  size_t GetBlockIndexSize() const;

  friend class BlockBuilderTest_Encode_Test;

private:
  std::vector<Byte> buffer_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_INDEX_H