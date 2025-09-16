#include "sstable/table_reader.h"

#include "io/linux_file.h"
#include "sstable/block_index.h"
#include "sstable/block_reader.h"

namespace kvs {
constexpr int kDefaultExtraInfoSize = 40; // Bytes
}

namespace kvs {

namespace sstable {

TableReader::TableReader(std::string &&filename, uint64_t file_size)
    : filename_(std::move(filename)), file_size_(file_size),
      read_file_object_(std::make_shared<io::LinuxReadOnlyFile>(filename_)) {}

bool TableReader::Open() {
  if (!read_file_object_->Open()) {
    return false;
  }

  uint64_t total_block_entries = 0, starting_meta_section_offset = 0,
           meta_section_length = 0;
  // Decode block index
  DecodeExtraInfo(&total_block_entries, &starting_meta_section_offset,
                  &meta_section_length);
  // Fill block index info into block_index_
  FetchBlockIndexInfo(total_block_entries, starting_meta_section_offset,
                      meta_section_length);

  return true;
}

void TableReader::DecodeExtraInfo(uint64_t *total_block_entries,
                                  uint64_t *starting_meta_section_offset,
                                  uint64_t *meta_section_length) {
  std::array<Byte, kDefaultExtraInfoSize> extra_info_buffer;

  // Get last 40 bytes
  uint64_t start_offset_extra_info = file_size_ - kDefaultExtraInfoSize - 1;

  ssize_t bytes_read =
      read_file_object_->RandomRead(extra_info_buffer, start_offset_extra_info);
  if (bytes_read < 0) {
    return;
  }

  // first 8 bytes contains info of total block entries in table
  *total_block_entries = *reinterpret_cast<uint64_t *>(&extra_info_buffer[0]);
  // byte 8 - 15 contains starting offset of meta section
  *starting_meta_section_offset =
      *reinterpret_cast<uint64_t *>(&extra_info_buffer[8]);
  // byte 16 - 23 contains length of meta section
  *meta_section_length = *reinterpret_cast<uint64_t *>(&extra_info_buffer[16]);
  // byte 24- 31 contains min transaction id
  min_transaction_id_ = *reinterpret_cast<uint64_t *>(&extra_info_buffer[24]);
  // byte 32 - 40 contain max transcation id
  max_transaction_id_ = *reinterpret_cast<uint64_t *>(&extra_info_buffer[32]);
}

void TableReader::FetchBlockIndexInfo(uint64_t total_block_entries,
                                      uint64_t starting_meta_section_offset,
                                      uint64_t meta_section_length) {
  assert(total_block_entries > 0 && total_block_entries < ULLONG_MAX);
  assert(starting_meta_section_offset > 0 &&
         starting_meta_section_offset < ULLONG_MAX);
  assert(meta_section_length > 0 && meta_section_length < ULLONG_MAX);

  std::vector<Byte> block_index_buffer(meta_section_length, 0);
  ssize_t bytes_read = read_file_object_->RandomRead(
      block_index_buffer, starting_meta_section_offset);
  if (bytes_read < 0) {
    return;
  }

  uint64_t starting_offset = 0;

  for (int i = 0; i < total_block_entries; i++) {
    // First 4 bytes contain info smallest key length
    const uint32_t smallest_key_length = *reinterpret_cast<const uint32_t *>(
        &block_index_buffer[starting_offset]);
    // Move block_index_current_offset to starting offset of key
    starting_offset += sizeof(uint32_t);

    // Get smallest key based on smallest key length info
    std::string_view block_smallest_key(
        reinterpret_cast<const char *>(&block_index_buffer[starting_offset]),
        smallest_key_length);

    // Update offset to point to starting index of largest key
    starting_offset += block_smallest_key.size();

    // Next 4 bytes contain info block largest key
    uint32_t largest_key_length = *reinterpret_cast<const uint32_t *>(
        &block_index_buffer[starting_offset]);

    // Move block_index_current_offset to starting offset of value
    starting_offset += sizeof(uint32_t);

    // Get largest key based on largest key length info
    std::string_view block_largest_key(
        reinterpret_cast<const char *>(&block_index_buffer[starting_offset]),
        largest_key_length);

    // Update offset to point to starting offset of corresponding data block
    // (8B)
    starting_offset += block_largest_key.size();

    // Next 8 bytes contain starting offset of corresponding data block
    const uint64_t block_starting_offset = *reinterpret_cast<const uint64_t *>(
        &block_index_buffer[starting_offset]);

    // Update offset to point to starting offset of corresponding data block
    // (8B)
    starting_offset += sizeof(uint64_t);

    // Next 8 bytes contain starting offset of corresponding data block
    const uint64_t block_length = *reinterpret_cast<const uint64_t *>(
        &block_index_buffer[starting_offset]);

    // Update offset to point to starting offset of next block index
    starting_offset += sizeof(uint64_t);

    // Cache block index info
    block_index_.emplace_back(block_smallest_key, block_largest_key,
                              block_starting_offset, block_length);
  }
}

db::GetStatus TableReader::SearchKey(std::string_view key, TxnId txn_id) const {
  // Find the block that have smallest largest key that >= key
  int left = 0;
  int right = block_index_.size();

  while (left < right) {
    int mid = left + (right - left) / 2;
    if (block_index_[mid].GetLargestKey() >= key) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }

  uint64_t block_offset = block_index_[right].GetBlockStartOffset();
  uint64_t block_size = block_index_[right].GetBlockSize();

  // TODO(namnh) : Should cache this block.
  auto block_reader =
      std::make_unique<BlockReader>(read_file_object_, block_size);
  db::GetStatus status = block_reader->SearchKey(block_offset, key, txn_id);

  return status;
}

const std::vector<BlockIndex> &TableReader::GetBlockIndex() const {
  return block_index_;
}

} // namespace sstable

} // namespace kvs