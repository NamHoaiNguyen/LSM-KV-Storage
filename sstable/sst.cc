#include "sstable/sst.h"

#include "db/config.h"
#include "db/memtable_iterator.h"
#include "io/base_file.h"
#include "io/linux_file.h"
#include "sstable/block.h"
#include "sstable/block_index.h"
#include "sstable/block_reader.h"

// libC++
#include <cassert>

namespace kvs {

namespace sstable {

Table::Table(std::string &&filename, uint64_t table_id,
             const db::Config *config)
    : filename_(std::move(filename)), table_id_(table_id),
      write_file_object_(std::make_unique<io::LinuxWriteOnlyFile>(filename_)),
      block_data_(std::make_unique<Block>()), current_offset_(0),
      min_txnid_(UINT64_MAX), max_txnid_(0), config_(config) {}

void Table::AddEntry(std::string_view key, std::optional<std::string_view> value,
                     TxnId txn_id, db::ValueType value_type) {
  if (table_smallest_key_.empty()) {
    table_smallest_key_ = std::string(key);
  }

  if (block_data_->GetBlockSize() == 0) {
    block_smallest_key_ = std::string(key);
  }

  block_data_->AddEntry(key, value, txn_id, value_type);

  // Update min/max transaction id of sst
  min_txnid_ = std::min(min_txnid_, txn_id);
  max_txnid_ = std::max(max_txnid_, txn_id);

  // Update block/table largest key
  block_largest_key_ = std::string(key);
  table_largest_key_ = std::string(key);

  if (block_data_->GetBlockSize() >= config_->GetSSTBlockSize()) {
    FlushBlock();
  }
}

void Table::FlushBlock() {
  assert(write_file_object_);

  // Starting offset of block
  const uint64_t block_starting_offset = current_offset_;

  // Flush data part of data block to disk
  std::span<const Byte> data_buffer = block_data_->GetDataView();
  write_file_object_->Append(data_buffer, current_offset_);
  current_offset_ += data_buffer.size();

  // Flush metadata part of data block to disk
  std::span<const Byte> offset_buffer = block_data_->GetOffsetView();
  write_file_object_->Append(offset_buffer, current_offset_);
  current_offset_ += offset_buffer.size();

  // Flush extra info of data block to disk
  block_data_->EncodeExtraInfo();
  std::span<const Byte> extra_buffer = block_data_->GetExtraView();
  write_file_object_->Append(extra_buffer, current_offset_);
  current_offset_ += extra_buffer.size();

  // Ensure that data is persisted to disk from page cache
  // TODO(namnh, IMPORTANCE) : Do we need to do that right now? it significantly
  // degrades performance
  // write_file_object_->Flush();

  // Build MetaEntry format (block_meta)
  AddIndexBlockEntry(block_smallest_key_, block_largest_key_,
                     block_starting_offset,
                     current_offset_ - block_starting_offset);

  // Cache block index
  block_index_.emplace_back(block_smallest_key_, block_largest_key_,
                            block_starting_offset,
                            current_offset_ - block_starting_offset);

  // Reset block data
  block_data_->Reset();
}

void Table::AddIndexBlockEntry(std::string_view first_key,
                               std::string_view last_key,
                               uint64_t block_start_offset,
                               uint64_t block_length) {
  // Safe, because we limit length of key is less than 2^32
  const uint32_t first_key_len = static_cast<uint32_t>(first_key.size());
  const Byte *const first_key_len_bytes =
      reinterpret_cast<const Byte *const>(&first_key_len);
  const Byte *const first_key_buff =
      reinterpret_cast<const Byte *const>(first_key.data());

  // Safe, because we limit length of key is less than 2^32
  const uint32_t last_key_len = static_cast<uint32_t>(last_key.size());
  const Byte *const last_key_len_bytes =
      reinterpret_cast<const Byte *>(&last_key_len);
  const Byte *const last_key_buff =
      reinterpret_cast<const Byte *const>(last_key.data());

  // Convert block_start_offset
  const Byte *const block_start_offset_buff =
      reinterpret_cast<const Byte *const>(&block_start_offset);

  // convert block_length
  const Byte *const block_length_buff =
      reinterpret_cast<const Byte *const>(&block_length);

  // Insert length of first key(4 Bytes)
  index_block_buffer_.insert(index_block_buffer_.end(), first_key_len_bytes,
                             first_key_len_bytes + sizeof(uint32_t));
  // Insert first key
  index_block_buffer_.insert(index_block_buffer_.end(), first_key_buff,
                             first_key_buff + first_key_len);
  // Insert length of last key(4 Bytes)
  index_block_buffer_.insert(index_block_buffer_.end(), last_key_len_bytes,
                             last_key_len_bytes + sizeof(uint32_t));
  // Insert last key
  index_block_buffer_.insert(index_block_buffer_.end(), last_key_buff,
                             last_key_buff + last_key_len);
  // Insert starting offset of block data
  index_block_buffer_.insert(index_block_buffer_.end(), block_start_offset_buff,
                             block_start_offset_buff + sizeof(uint64_t));
  // Insert length of block data
  index_block_buffer_.insert(index_block_buffer_.end(), block_length_buff,
                             block_length_buff + sizeof(uint64_t));
}

void Table::Finish() {
  // Flush remaining data to
  FlushBlock();

  // Write block_index_ to page cache
  std::span<const Byte> block_index_buffer = index_block_buffer_;
  // current_offset now is starting offset of block section
  ssize_t block_index_size =
      write_file_object_->Append(block_index_buffer, current_offset_);
  if (block_index_size < 0) {
    throw std::runtime_error("Error when flushing meta section of sstable");
  }

  EncodeExtraInfo();

  // Ensure that data is persisted to disk from page cache
  if (write_file_object_->Flush()) {
    // All data in SST is persisted to disk. Free file object
    // to not allow any writing
    write_file_object_.reset();
  }

  // Maybe should open file for reading only for using later?
  if (!read_file_object_) {
    read_file_object_ = std::make_shared<io::LinuxReadOnlyFile>(filename_);
    // TODO(namnh) : recheck. It can exhaust file descriptors
    read_file_object_->Open();
  }
}

void Table::EncodeExtraInfo() {
  const Byte *const meta_section_offset_bytes =
      reinterpret_cast<const Byte *const>(&current_offset_);
  // Insert starting offset of meta section
  extra_buffer_.insert(extra_buffer_.end(), meta_section_offset_bytes,
                       meta_section_offset_bytes + sizeof(uint64_t));

  // Insert size of meta section
  uint64_t block_index_size_u64 =
      static_cast<uint64_t>(index_block_buffer_.size());
  const Byte *const meta_section_len =
      reinterpret_cast<const Byte *const>(&block_index_size_u64);
  extra_buffer_.insert(extra_buffer_.end(), meta_section_len,
                       meta_section_len + sizeof(uint64_t));

  // Insert min txnid
  const Byte *const min_txnid_bytes =
      reinterpret_cast<const Byte *const>(&min_txnid_);
  extra_buffer_.insert(extra_buffer_.end(), min_txnid_bytes,
                       min_txnid_bytes + sizeof(uint64_t));

  // Insert max txnid
  const Byte *const max_txnid_bytes =
      reinterpret_cast<const Byte *const>(&max_txnid_);
  extra_buffer_.insert(extra_buffer_.end(), max_txnid_bytes,
                       max_txnid_bytes + sizeof(uint64_t));
}

bool Table::Open() {
  if (!write_file_object_) {
    return false;
  }

  return write_file_object_->Open();
}

void Table::Read() {
  if (!read_file_object_) {
    read_file_object_ = std::make_unique<io::LinuxReadOnlyFile>(filename_);
  }
}

db::GetStatus Table::SearchKey(std::string_view key, TxnId txn_id) {
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
      std::make_unique<BlockReader>(filename_, read_file_object_);
  db::GetStatus status =
      block_reader->SearchKey(block_offset, block_size, key, txn_id);

  return status;
}

std::string_view Table::GetSmallestKey() const { return table_smallest_key_; }

std::string_view Table::GetLargestKey() const { return table_largest_key_; }

uint64_t Table::GetTableId() const { return table_id_; };

// For testing
Block *Table::GetBlockData() { return block_data_.get(); };

io::WriteOnlyFile *Table::GetWriteOnlyFileObject() {
  return write_file_object_.get();
}

} // namespace sstable

} // namespace kvs