#include "sstable/block_reader.h"

#include "io/buffer.h"
#include "io/linux_file.h"

#include <cassert>

namespace kvs {

namespace sstable {


// BlockReader::BlockReader(std::shared_ptr<io::ReadOnlyFile> read_file_object,
//                          size_t size) {
//   assert(size > 0);
//   buffer_.resize(size);
// }

BlockReader::BlockReader(std::unique_ptr<BlockReaderData> block_reader_data)
    : total_data_entries_(block_reader_data->total_data_entries),
      offset_section_(block_reader_data->offset_section),
      data_entries_offset_info_(
          std::move(block_reader_data->data_entries_offset_info)),
      buffer_(std::move(block_reader_data->buffer)) {}

// bool BlockReader::FetchBlockData(BlockOffset offset) {
//   if (offset < 0) {
//     return false;
//   }

//   ssize_t bytes_read = read_file_object_->RandomRead(buffer_, offset);
//   if (bytes_read < 0) {
//     return false;
//   }

//   int64_t last_block_offset = buffer_.size() - 1;
//   // 16 last bytes of lock contain metadata info(num entries + starting offset
//   // of offset section)
//   total_data_entries_ =
//       *reinterpret_cast<uint64_t *>(&buffer_[last_block_offset - 15]);
//   offset_section_ =
//       *reinterpret_cast<uint64_t *>(&buffer_[last_block_offset - 7]);

//   for (uint64_t i = 0; i < total_data_entries_; i++) {
//     data_entries_offset_info_.emplace_back(GetDataEntryOffset(i));
//   }

//   return true;
// }

db::GetStatus BlockReader::SearchKey(std::string_view key, TxnId txn_id) const {
  db::GetStatus status;

  // Binary search key in block based on offset
  uint64_t left = 0;
  uint64_t right = total_data_entries_;

  while (left <= right) {
    uint64_t mid = left + (right - left) / 2;

    // Get value type of data entry
    uint64_t data_entry_offset = GetDataEntryOffset(mid);
    db::ValueType value_type = GetValueTypeFromDataEntry(data_entry_offset);
    assert(value_type == db::ValueType::PUT ||
           value_type == db::ValueType::DELETED);

    // Get key and(or) value of data entry
    auto key_in_block = GetKeyFromDataEntry(data_entry_offset);
    if (key_in_block == key) {
      status.type = value_type;
      if (status.type == db::ValueType::DELETED) {
        // entry is deleted, value of entry is empty
        status.value = std::nullopt;
        return status;
      }

      status.value = GetValueFromDataEntry(data_entry_offset);
      return status;
    } else if (key_in_block < key) {
      left = mid + 1;
    } else {
      if (mid == 0) {
        // Special case, because uint64_t always >= 0. So -1 will be casted
        // to 2^64 - 1
        break;
      }

      right = mid - 1;
    }
  }

  return status;
}

uint64_t BlockReader::GetDataEntryOffset(int entry_index) const {
  // Starting offset of offset entry at index (entry_index) (th)
  uint64_t offset_entry = offset_section_ + entry_index * 2 * sizeof(uint64_t);

  const uint64_t data_entry_offset =
      *reinterpret_cast<const uint64_t *>(&buffer_[offset_entry]);

  return data_entry_offset;
}

db::ValueType
BlockReader::GetValueTypeFromDataEntry(uint64_t data_entry_offset) const {
  Byte value_type_byte = buffer_[data_entry_offset];
  db::ValueType value_type =
      *reinterpret_cast<const db::ValueType *>(&value_type_byte);

  return value_type;
}

std::string_view
BlockReader::GetKeyFromDataEntry(uint64_t data_entry_offset) const {
  const uint32_t key_len = *reinterpret_cast<const uint32_t *>(
      &buffer_[data_entry_offset + sizeof(uint8_t)]);

  uint64_t start_offset_key =
      data_entry_offset + sizeof(uint8_t) + sizeof(uint32_t);

  std::string_view key(
      reinterpret_cast<const char *>(&buffer_[start_offset_key]), key_len);

  return key;
}

std::string_view
BlockReader::GetValueFromDataEntry(uint64_t data_entry_offset) const {
  std::string_view key = GetKeyFromDataEntry(data_entry_offset);

  uint64_t start_offset_value_len =
      data_entry_offset + sizeof(uint8_t) + sizeof(uint32_t) + key.size();
  const uint32_t value_len =
      *reinterpret_cast<const uint32_t *>(&buffer_[start_offset_value_len]);

  uint64_t start_offset_value = start_offset_value_len + sizeof(uint32_t);
  std::string_view value(
      reinterpret_cast<const char *>(&buffer_[start_offset_value]), value_len);

  return value;
}

} // namespace sstable

} // namespace kvs