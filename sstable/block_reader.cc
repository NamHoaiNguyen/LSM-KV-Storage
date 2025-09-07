#include "sstable/block_reader.h"

#include "db/status.h"
#include "io/buffer.h"
#include "io/linux_file.h"

#include <iostream>

namespace kvs {

namespace sstable {

BlockReader::BlockReader(std::string_view filename,
                         std::shared_ptr<io::ReadOnlyFile> read_file_object)
    : read_file_object_(read_file_object) {}

void BlockReader::SearchKey(uint64_t offset, uint64_t size,
                            std::string_view key, TxnId txn_id) {
  if (offset < 0 || size < 0) {
    return;
  }

  ssize_t bytes_read = read_file_object_->RandomRead(offset, size);
  if (bytes_read < 0) {
    return;
  }

  io::Buffer *buffer = read_file_object_->GetBuffer();
  if (!buffer) {
    return;
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

  while (left < right) {
    uint64_t mid = left + (right - left) / 2;
    uint64_t data_entry_offset =
        GetDataEntryOffset(buffer_view, offset_offset_section, mid);
    std::string key_in_block =
        GetKeyAtFromDataEntry(buffer_view, data_entry_offset);
    if (key_in_block == key) {
      return;
    } else if (key_in_block < key) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
}

const uint64_t BlockReader::GetDataEntryOffset(std::span<const Byte> buffer,
                                               const uint64_t offset_section,
                                               const int entry_index) {
  uint64_t offset_entry = offset_section + entry_index * 2 * sizeof(uint64_t);

  const uint64_t data_entry_offset =
      *reinterpret_cast<const uint64_t *>(&buffer[offset_entry]);

  return data_entry_offset;
}

std::string
BlockReader::GetKeyAtFromDataEntry(std::span<const Byte> buffer_view,
                                   uint64_t data_entry_offset) {
  const Byte value_type_byte = buffer_view[data_entry_offset];
  const db::ValueType value_type =
      *reinterpret_cast<const db::ValueType *>(&value_type_byte);
  const uint32_t key_len =
      *reinterpret_cast<const uint32_t *>(&buffer_view[data_entry_offset + 1]);

  uint64_t start_offset_key =
      data_entry_offset + sizeof(uint8_t) + sizeof(uint32_t);

  std::string_view sv(
      reinterpret_cast<const char *>(&buffer_view[start_offset_key]), key_len);

  return "";
}

} // namespace sstable

} // namespace kvs