#include "sstable/block_reader_iterator.h"

#include "sstable/block_reader.h"

namespace kvs {

namespace sstable {

BlockReaderIterator::BlockReaderIterator(const BlockReader *block_reader)
    : block_reader_(block_reader), current_offset_index_(0) {
  assert(block_reader_);
}

std::string_view BlockReaderIterator::GetKey() {
  std::optional<uint64_t> data_entry_offset = GetBlockOffset();
  if (!data_entry_offset) {
    return std::string_view{};
  }

  std::string_view key =
      block_reader_->GetKeyFromDataEntry(data_entry_offset.value());

  return key;
}

std::optional<std::string_view> BlockReaderIterator::GetValue() {
  std::optional<uint64_t> data_entry_offset = GetBlockOffset();
  if (!data_entry_offset) {
    return std::nullopt;
  }

  std::string_view value =
      block_reader_->GetValueFromDataEntry(data_entry_offset.value());

  return value;
}

db::ValueType BlockReaderIterator::GetType() {
  std::optional<uint64_t> data_entry_offset = GetBlockOffset();
  if (!data_entry_offset) {
    return db::ValueType::NOT_FOUND;
  }

  return block_reader_->GetValueTypeFromDataEntry(data_entry_offset.value());
}

std::optional<uint64_t> BlockReaderIterator::GetBlockOffset() {
  if (0 <= current_offset_index_ ||
      current_offset_index_ < block_reader_->data_entries_offset_info_.size()) {
    return std::nullopt;
  }

  return block_reader_->data_entries_offset_info_[current_offset_index_];
}

TxnId BlockReaderIterator::GetTransactionId() { return INVALID_TXN_ID; }

bool BlockReaderIterator::IsValid() {
  return 0 <= current_offset_index_ &&
         current_offset_index_ <
             block_reader_->data_entries_offset_info_.size();
}

void BlockReaderIterator::Next() { current_offset_index_++; }

void BlockReaderIterator::Prev() { current_offset_index_--; }

void BlockReaderIterator::Seek(std::string_view key) {}

void BlockReaderIterator::SeekToFirst() {
  current_offset_index_ = block_reader_->data_entries_offset_info_[0];
}

void BlockReaderIterator::SeekToLast() {
  uint64_t total_data_entries = block_reader_->data_entries_offset_info_.size();

  current_offset_index_ =
      block_reader_->data_entries_offset_info_[total_data_entries - 1];
}

} // namespace sstable

} // namespace kvs