#include <sstable/block_builder.h>

#include <cassert>

namespace kvs {

namespace sstable {

BlockBuilder::BlockBuilder()
    : block_size_(0), num_entries_(0), data_current_offset_(0) {}

void BlockBuilder::AddEntry(std::string_view key, std::string_view value,
                            TxnId txn_id, db::ValueType value_type) {
  assert(key.data());

  // Add entry into data entry section
  EncodeDataEntry(key, value, txn_id, value_type);

  size_t data_entry_size =
      sizeof(uint8_t) + sizeof(uint32_t) + key.size() +
      (value.data() ? sizeof(uint32_t) + value.size() : 0) + sizeof(TxnId);

  // Add offset info of this entry into offset section
  EncodeOffsetEntry(data_current_offset_, data_entry_size);

  // Update num_entries
  num_entries_++;

  // / Update current_offset_
  data_current_offset_ += data_entry_size;

  // Update block's current size
  block_size_ += data_entry_size + 2 * sizeof(uint64_t);
}

void BlockBuilder::EncodeDataEntry(std::string_view key, std::string_view value,
                                   TxnId txn_id, db::ValueType value_type) {
  assert(key.size() <= kMaxKeySize);

  // Insert value type
  Byte value_type_bytes = static_cast<Byte>(value_type);
  data_buffer_.insert(data_buffer_.end(), value_type_bytes);

  // Insert length of key(4 bytes)
  // Safe, because we limit length of key is less than 2^32
  const uint32_t key_len = static_cast<uint32_t>(key.size());
  const Byte *const key_len_bytes =
      reinterpret_cast<const Byte *const>(&key_len);
  data_buffer_.insert(data_buffer_.end(), key_len_bytes,
                      key_len_bytes + sizeof(uint32_t));

  // Insert key
  const Byte *const key_bytes = reinterpret_cast<const Byte *const>(key.data());
  data_buffer_.insert(data_buffer_.end(), key_bytes, key_bytes + key_len);

  if (value.data()) {
    assert(value_type == db::ValueType::PUT);
    // Insert length of value(4 bytes)
    // Safe, because we limit length of key is less than 2^32
    const uint32_t value_len = static_cast<uint32_t>(value.size());
    const Byte *const value_len_bytes =
        reinterpret_cast<const Byte *>(&value_len);
    data_buffer_.insert(data_buffer_.end(), value_len_bytes,
                        value_len_bytes + sizeof(uint32_t));

    // Insert value
    const Byte *const value_bytes =
        reinterpret_cast<const Byte *const>(value.data());
    data_buffer_.insert(data_buffer_.end(), value_bytes,
                        value_bytes + value_len);
  }
  // Insert transaction id
  const Byte *const transaction_id_bytes =
      reinterpret_cast<const Byte *const>(&txn_id);
  data_buffer_.insert(data_buffer_.end(), transaction_id_bytes,
                      transaction_id_bytes + sizeof(TxnId));
}

void BlockBuilder::EncodeOffsetEntry(size_t start_entry_offset,
                                     size_t data_entry_size) {
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

void BlockBuilder::EncodeExtraInfo() {
  uint64_t start_offset_offset_section = data_current_offset_;

  const Byte *const num_entries_buff =
      reinterpret_cast<const Byte *const>(&num_entries_);
  const Byte *const start_offset_offset_section_buff =
      reinterpret_cast<const Byte *const>(&start_offset_offset_section);

  // Insert num entries
  extra_buffer_.insert(extra_buffer_.end(), num_entries_buff,
                       num_entries_buff + sizeof(uint64_t));
  // Insert starting offset of offset secion
  extra_buffer_.insert(extra_buffer_.end(), start_offset_offset_section_buff,
                       start_offset_offset_section_buff + sizeof(uint64_t));
}

void BlockBuilder::Reset() {
  block_size_ = 0;
  num_entries_ = 0;
  data_buffer_.clear();
  offset_buffer_.clear();
  extra_buffer_.clear();
  data_current_offset_ = 0;
}

size_t BlockBuilder::GetBlockSize() const { return block_size_; }

std::span<const Byte> BlockBuilder::GetDataView() { return data_buffer_; }

std::span<const Byte> BlockBuilder::GetOffsetView() { return offset_buffer_; }

std::span<const Byte> BlockBuilder::GetExtraView() { return extra_buffer_; }

uint64_t BlockBuilder::GetNumEntries() const { return num_entries_; }

} // namespace sstable

} // namespace kvs