#include <sstable/block_builder.h>

#include <cassert>

namespace kvs {

BlockBuilder::BlockBuilder()
    : is_finished_(false), block_size_(0), num_entries_(0), current_offset_(0) {
}

void BlockBuilder::AddEntry(std::string_view key, std::string_view value,
                     TxnId txn_id) {
  if (is_finished_) {
    return;
  }

  // Add entry into data entry section
  AddDataEntry(key, value, txn_id);

  size_t data_entry_size = sizeof(uint32_t) + key.size() + sizeof(uint32_t) +
                           value.size() + sizeof(TxnId);

  size_t start_entry_offset = current_offset_;
  current_offset_ += data_entry_size;

  // Add offset info of this entry into offset section
  AddOffsetEntry(start_entry_offset, data_entry_size);

  // Update num_entries
  num_entries_++;

  // Update current_offset_
  current_offset_ += 2 * sizeof(uint64_t);

  // Update block's current size
  block_size_ += data_entry_size + 2 * sizeof(uint64_t);
}

void BlockBuilder::AddDataEntry(std::string_view key, std::string_view value,
                         TxnId txn_id) {
  assert(key.size() <= kMaxKeySize);
  // Safe, because we limit length of key is less than 2^32
  const uint32_t key_len = static_cast<uint32_t>(key.size());
  const Byte *const key_len_bytes =
      reinterpret_cast<const Byte *const>(&key_len);
  // EncodeFixed32(dst, key_len);
  const Byte *const key_buff = reinterpret_cast<const Byte *const>(key.data());

  // Safe, because we limit length of key is less than 2^32
  const uint32_t value_len = static_cast<uint32_t>(value.size());
  const Byte *const value_len_bytes =
      reinterpret_cast<const Byte *>(&value_len);
  const Byte *const value_buff =
      reinterpret_cast<const Byte *const>(value.data());

  // Transaction Id
  const Byte *const transcation_buff =
      reinterpret_cast<const Byte *const>(&txn_id);

  // Insert data with order based on block format
  // Insert length of key(4 bytes)
  data_buffer_.insert(data_buffer_.end(), key_len_bytes,
                      key_len_bytes + sizeof(uint32_t));
  // Insert key
  data_buffer_.insert(data_buffer_.end(), key_buff, key_buff + key_len);
  // Insert length of value(4 bytes)
  data_buffer_.insert(data_buffer_.end(), value_len_bytes,
                      value_len_bytes + sizeof(uint32_t));
  // Insert value
  data_buffer_.insert(data_buffer_.end(), value_buff, value_buff + value_len);
  // Insert transaction id
  data_buffer_.insert(data_buffer_.end(), transcation_buff,
                      transcation_buff + sizeof(TxnId));
}

void BlockBuilder::AddOffsetEntry(size_t start_entry_offset, size_t data_entry_size) {
  const Byte *const start_entry_offset_buff =
      reinterpret_cast<const Byte *const>(&start_entry_offset);
  uint64_t data_entry_size_bytes = static_cast<uint64_t>(data_entry_size);
  const Byte *const data_entry_size_buff =
      reinterpret_cast<const Byte *const>(&data_entry_size_bytes);

  // Insert starting offset of data entry
  offset_buffer_.insert(offset_buffer_.end(), start_entry_offset_buff,
                        start_entry_offset_buff + sizeof(uint64_t));
  // Insert length of data entry
  offset_buffer_.insert(offset_buffer_.end(), data_entry_size_buff,
                        data_entry_size_buff + sizeof(uint64_t));
}

void BlockBuilder::Finish() {
  is_finished_ = true;

  // Insert total number entries of data block
  // TODO(namnh) : Do we need this info?
}

void BlockBuilder::Reset() {
  is_finished_ = false;
  block_size_ = 0;
  num_entries_ = 0;
  data_buffer_.clear();
}

size_t BlockBuilder::GetBlockSize() const { return block_size_; }

std::span<const Byte> BlockBuilder::GetDataView() { return data_buffer_; }

std::span<const Byte> BlockBuilder::GetOffsetView() { return offset_buffer_; }

uint64_t BlockBuilder::GetNumEntries() const { return num_entries_; }

} // namespace kvs