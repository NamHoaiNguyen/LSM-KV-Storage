#include "sstable/block_reader.h"

#include "io/buffer.h"
#include "io/linux_file.h"

#include <cassert>

namespace kvs {

namespace sstable {

BlockReader::BlockReader(std::string_view filename,
                         std::shared_ptr<io::ReadOnlyFile> read_file_object)
    : read_file_object_(read_file_object) {}

db::GetStatus BlockReader::SearchKey(uint64_t offset, uint64_t size,
                                     std::string_view key, TxnId txn_id) const {
  db::GetStatus status;

  if (offset < 0 || size < 0) {
    return status;
  }

  ssize_t bytes_read = read_file_object_->RandomRead(offset, size);
  if (bytes_read < 0) {
    return status;
  }

  io::Buffer *buffer = read_file_object_->GetBuffer();
  if (!buffer) {
    return status;
  }

  std::span<const Byte> buffer_view = buffer->GetImmutableBufferView();

  int64_t last_block_offset = size - 1;
  // 16 last bytes of lock contain metadata info(num entries + starting offset
  // of offset section)
  const uint64_t block_num_entries =
      *reinterpret_cast<const uint64_t *>(&buffer_view[last_block_offset - 15]);
  const uint64_t offset_offset_section =
      *reinterpret_cast<const uint64_t *>(&buffer_view[last_block_offset - 7]);

  // Binary search key in block based on offset
  uint64_t left = 0;
  uint64_t right = block_num_entries;

  // std::pair<std::string_view, std::optional<std::string_view>> test;

  while (left <= right) {
    uint64_t mid = left + (right - left) / 2;

    // Get value type of data entry
    uint64_t data_entry_offset =
        GetDataEntryOffset(buffer_view, offset_offset_section, mid);
    db::ValueType value_type =
        GetValueTypeFromDataEntry(buffer_view, data_entry_offset);

    // Get key and(or) value of data entry
    auto [key_in_block, value_in_block] =
        GetKeyValueFromDataEntry(buffer_view, data_entry_offset, value_type);
    if (key_in_block == key) {
      status.type = value_type;
      status.value =
          (value_in_block)
              ? std::make_optional<std::string>(value_in_block.value())
              : std::nullopt;
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

uint64_t BlockReader::GetDataEntryOffset(std::span<const Byte> buffer,
                                         const uint64_t offset_section,
                                         const int entry_index) const {
  // Starting offset of offset entry at index (entry_index) (th)
  uint64_t offset_entry = offset_section + entry_index * 2 * sizeof(uint64_t);

  const uint64_t data_entry_offset =
      *reinterpret_cast<const uint64_t *>(&buffer[offset_entry]);

  return data_entry_offset;
}

db::ValueType
BlockReader::GetValueTypeFromDataEntry(std::span<const Byte> buffer_view,
                                       uint64_t data_entry_offset) const {
  Byte value_type_byte = buffer_view[data_entry_offset];
  db::ValueType value_type =
      *reinterpret_cast<const db::ValueType *>(&value_type_byte);

  return value_type;
}

std::pair<std::string_view, std::optional<std::string_view>>
BlockReader::GetKeyValueFromDataEntry(std::span<const Byte> buffer_view,
                                      uint64_t data_entry_offset,
                                      db::ValueType value_type) const {
  assert(value_type != db::ValueType::INVALID);

  const uint32_t key_len = *reinterpret_cast<const uint32_t *>(
      &buffer_view[data_entry_offset + sizeof(uint8_t)]);

  uint64_t start_offset_key =
      data_entry_offset + sizeof(uint8_t) + sizeof(uint32_t);

  std::string_view key(
      reinterpret_cast<const char *>(&buffer_view[start_offset_key]), key_len);

  if (value_type == db::ValueType::DELETED) {
    return {key, std::nullopt};
  }

  uint64_t start_offset_value_len = start_offset_key + key.size();
  const uint32_t value_len =
      *reinterpret_cast<const uint32_t *>(&buffer_view[start_offset_value_len]);

  uint64_t start_offset_value = start_offset_value_len + sizeof(uint32_t);
  std::string_view value(
      reinterpret_cast<const char *>(&buffer_view[start_offset_value]),
      value_len);

  return {key, value};
}

} // namespace sstable

} // namespace kvs