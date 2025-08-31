#include "sstable/block_index.h"

namespace kvs {

namespace sstable {

void BlockIndex::AddEntry(std::string_view first_key, std::string_view last_key,
                          uint64_t block_start_offset, uint64_t block_length) {
  // Safe, because we limit length of key is less than 2^32
  const uint32_t first_key_len = static_cast<uint32_t>(first_key.size());
  const Byte *const first_key_len_bytes =
      reinterpret_cast<const Byte *const>(&first_key_len);
  const Byte *const first_key_buff =
      reinterpret_cast<const Byte *const>(first_key.data());

  // Safe, because we limit length of key is less than 2^32
  const uint32_t last_key_len = static_cast<uint32_t>(last_key.size());
  const Byte *const last_key_len_bytes =
      reinterpret_cast<const Byte *>(&last_key_len);
  const Byte *const last_key_buff =
      reinterpret_cast<const Byte *const>(last_key.data());

  // Convert block_start_offset
  const Byte *const block_start_offset_buff =
      reinterpret_cast<const Byte *const>(&block_start_offset);

  // convert block_length
  const Byte *const block_length_buff =
      reinterpret_cast<const Byte *const>(&block_length);

  // Insert length of first key(4 Bytes)
  buffer_.insert(buffer_.end(), first_key_len_bytes,
                 first_key_len_bytes + sizeof(uint32_t));
  // Insert first key
  buffer_.insert(buffer_.end(), first_key_buff, first_key_buff + first_key_len);
  // Insert length of last key(4 Bytes)
  buffer_.insert(buffer_.end(), last_key_len_bytes,
                 last_key_len_bytes + sizeof(uint32_t));
  // Insert last key
  buffer_.insert(buffer_.end(), last_key_buff, last_key_buff + last_key_len);
  // Insert starting offset of block data
  buffer_.insert(buffer_.end(), block_start_offset_buff,
                 block_start_offset_buff + sizeof(uint64_t));
  // Insert length of block data
  buffer_.insert(buffer_.end(), block_length_buff,
                 block_length_buff + sizeof(uint64_t));
}

std::span<const Byte> BlockIndex::GetBufferView() { return buffer_; }

size_t BlockIndex::GetBlockIndexSize() const { return buffer_.size(); }

} // namespace sstable

} // namespace kvs