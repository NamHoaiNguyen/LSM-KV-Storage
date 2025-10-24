#include "sstable/block_reader_iterator.h"

#include "sstable/block_reader.h"
#include "sstable/lru_block_item.h"

namespace kvs {

namespace sstable {

BlockReaderIterator::BlockReaderIterator(
    std::shared_ptr<LRUBlockItem> lru_block_item)
    : lru_block_item_(lru_block_item),
      block_reader_(lru_block_item->GetBlockReader()),
      current_offset_index_(0) {
  assert(block_reader_);
}

BlockReaderIterator::~BlockReaderIterator() {
  // if (auto tmpPtr = lru_block_item_.lock()) {
  //   tmpPtr->Unref();
  // }
  lru_block_item_->Unref();
}

std::optional<uint64_t> BlockReaderIterator::GetCurrentDataEntryOffset() {
  if (current_offset_index_ < 0 ||
      current_offset_index_ >=
          block_reader_->data_entries_offset_info_.size()) {
    return std::nullopt;
  }

  return block_reader_->data_entries_offset_info_[current_offset_index_];
}

std::string_view BlockReaderIterator::GetKey() {
  std::optional<uint64_t> data_entry_offset = GetCurrentDataEntryOffset();
  if (!data_entry_offset) {
    return std::string_view{};
  }

  std::string_view key =
      block_reader_->GetKeyFromDataEntry(data_entry_offset.value());

  return key;
}

std::string_view BlockReaderIterator::GetValue() {
  std::optional<uint64_t> data_entry_offset = GetCurrentDataEntryOffset();
  if (!data_entry_offset) {
    return std::string_view{};
  }

  std::string_view value =
      block_reader_->GetValueFromDataEntry(data_entry_offset.value());

  return value;
}

db::ValueType BlockReaderIterator::GetType() {
  std::optional<uint64_t> data_entry_offset = GetCurrentDataEntryOffset();
  if (!data_entry_offset) {
    return db::ValueType::NOT_FOUND;
  }

  return block_reader_->GetValueTypeFromDataEntry(data_entry_offset.value());
}

TxnId BlockReaderIterator::GetTransactionId() {
  std::optional<uint64_t> data_entry_offset = GetCurrentDataEntryOffset();
  if (!data_entry_offset) {
    return INVALID_TXN_ID;
  }

  return block_reader_->GetTransactionIdFromDataEntry(
      data_entry_offset.value());
}

bool BlockReaderIterator::IsValid() {
  return 0 <= current_offset_index_ &&
         current_offset_index_ <
             block_reader_->data_entries_offset_info_.size();
}

void BlockReaderIterator::Next() { current_offset_index_++; }

void BlockReaderIterator::Prev() { current_offset_index_--; }

void BlockReaderIterator::Seek(std::string_view key) {
  if (!key.data()) {
    return;
  }

  int64_t left = 0;
  int64_t right = block_reader_->total_data_entries_ - 1;

  while (left < right) {
    int64_t mid = left + (right - left) / 2;
    uint64_t data_entry_offset = block_reader_->data_entries_offset_info_[mid];
    std::string_view key_found =
        block_reader_->GetKeyFromDataEntry(data_entry_offset);

    if (key_found == key) {
      current_offset_index_ = mid;
      return;
    } else if (key_found > key) {
      right = mid;
      if (mid == 0) {
        // Special case, because uint64_t always >= 0. So -1 will be casted
        // to 2^64 - 1
        break;
      }
    } else {
      left = mid + 1;
    }
  }

  current_offset_index_ = left;
}

void BlockReaderIterator::SeekToFirst() { current_offset_index_ = 0; }

void BlockReaderIterator::SeekToLast() {
  current_offset_index_ = block_reader_->data_entries_offset_info_.size() - 1;
}

} // namespace sstable

} // namespace kvs