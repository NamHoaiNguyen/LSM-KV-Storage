#include "sstable/table_reader.h"

#include "io/linux_file.h"

namespace kvs {

namespace sstable {

TableReader::TableReader(std::string &&filename)
    : filename_(std::move(filename)),
      read_file_object_(std::make_shared<io::LinuxReadOnlyFile>(filename_)) {}

bool TableReader::Open() {
  if (!read_file_object_->Open()) {
    return false;
  }

  return true;
}

db::GetStatus Table::SearchKey(std::string_view key, TxnId txn_id) const {
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

std::string_view Table::GetSmallestKey() const { return table_smallest_key_; }

std::string_view Table::GetLargestKey() const { return table_largest_key_; }

uint64_t Table::GetTableId() const { return table_id_; };

} // namespace sstable

} // namespace kvs