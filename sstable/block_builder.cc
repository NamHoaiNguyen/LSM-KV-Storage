#include <sstable/block_builder.h>

#include <cassert>

namespace kvs {

BlockBuilder::BlockBuilder()
    : is_finished_(false), block_size_(0), num_entries_(0) {}

void BlockBuilder::Add(std::string_view key, std::string_view value,
                       TxnId txn_id) {
  if (is_finished_) {
    return;
  }

  assert(key.size() <= kMaxKeySize);
  // Safe, because we limit length of key is less than 2^32
  const uint32_t key_len = static_cast<uint32_t>(key.size());
  const Byte *key_len_bytes = reinterpret_cast<const Byte *>(&key_len);
  // EncodeFixed32(dst, key_len);
  const Byte *key_buff = reinterpret_cast<const Byte *>(key.data());

  // Safe, because we limit length of key is less than 2^32
  const uint32_t value_len = static_cast<uint32_t>(value.size());
  const Byte *value_len_bytes = reinterpret_cast<const Byte *>(&value_len);
  const Byte *value_buff = reinterpret_cast<const Byte *>(value.data());

  // Transaction Id
  const Byte *transcation_buff = reinterpret_cast<const Byte *>(&txn_id);

  // Insert data with order based on block format
  // Insert length of key(4 bytes)
  buffer_.insert(buffer_.end(), key_len_bytes,
                 key_len_bytes + sizeof(uint32_t));
  // Insert key
  buffer_.insert(buffer_.end(), key_buff, key_buff + key_len);
  // Insert length of value(4 bytes)
  buffer_.insert(buffer_.end(), value_len_bytes,
                 value_len_bytes + sizeof(uint32_t));
  // Insert value
  buffer_.insert(buffer_.end(), value_buff, value_buff + value_len);
  // Insert transaction id
  buffer_.insert(buffer_.end(), transcation_buff,
                 transcation_buff + sizeof(TxnId));

  // Update block's num_entries
  num_entries_++;

  // Update block'scurrent size
  block_size_ += sizeof(uint32_t) + key.size() + sizeof(uint32_t) +
                 value.size() + sizeof(TxnId);
}

void BlockBuilder::Finish() { is_finished_ = true; }

size_t BlockBuilder::GetBlockSize() const { return block_size_; }

uint64_t BlockBuilder::GetNumEntries() const { return num_entries_; }

} // namespace kvs