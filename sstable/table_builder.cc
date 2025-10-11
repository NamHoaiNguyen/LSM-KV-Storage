#include "sstable/table_builder.h"

#include "db/config.h"
#include "db/memtable_iterator.h"
#include "io/base_file.h"
#include "io/linux_file.h"
#include "sstable/block_builder.h"
#include "sstable/block_index.h"
#include "sstable/block_reader.h"

// libC++
#include <cassert>
#include <iostream>

namespace kvs {

namespace sstable {

TableBuilder::TableBuilder(std::string &&filename, const db::Config *config)
    : filename_(std::move(filename)),
      write_file_object_(std::make_unique<io::LinuxWriteOnlyFile>(filename_)),
      block_data_(std::make_unique<BlockBuilder>()), current_offset_(0),
      min_txnid_(UINT64_MAX), max_txnid_(0), total_block_entries_(0),
      data_size_(0), config_(config) {}

bool TableBuilder::Open() {
  if (!write_file_object_) {
    return false;
  }

  return write_file_object_->Open();
}

void TableBuilder::AddEntry(std::string_view key, std::string_view value,
                            TxnId txn_id, db::ValueType value_type) {
  // assert(value_type != db::ValueType::INVALID && txn_id != INVALID_TXN_ID);

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

  data_size_ += key.size() + (value.data() ? value.size() : 0);

  if (block_data_->GetBlockSize() >= config_->GetSSTBlockSize()) {
    FlushBlock();
  }
}

void TableBuilder::FlushBlock() {
  assert(write_file_object_);

  if (block_data_->GetDataView().empty()) {
    // Don't add new entry if data_buffer doesn't have any data
    return;
  }

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

  // Increase number of total block entries of table
  total_block_entries_++;

  // Reset block data
  block_data_->Reset();
}

void TableBuilder::AddIndexBlockEntry(std::string_view first_key,
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
  block_index_buffer_.insert(block_index_buffer_.end(), first_key_len_bytes,
                             first_key_len_bytes + sizeof(uint32_t));
  // Insert first key
  block_index_buffer_.insert(block_index_buffer_.end(), first_key_buff,
                             first_key_buff + first_key_len);
  // Insert length of last key(4 Bytes)
  block_index_buffer_.insert(block_index_buffer_.end(), last_key_len_bytes,
                             last_key_len_bytes + sizeof(uint32_t));
  // Insert last key
  block_index_buffer_.insert(block_index_buffer_.end(), last_key_buff,
                             last_key_buff + last_key_len);
  // Insert starting offset of block data
  block_index_buffer_.insert(block_index_buffer_.end(), block_start_offset_buff,
                             block_start_offset_buff + sizeof(uint64_t));
  // Insert length of block data
  block_index_buffer_.insert(block_index_buffer_.end(), block_length_buff,
                             block_length_buff + sizeof(uint64_t));

  // file_size_ += sizeof(uint32_t) + first_key.size() + sizeof(uint32_t) +
  //               last_key.size() + 2 * sizeof(uint64_t);
}

void TableBuilder::Finish() {
  // Flush remaining data to
  FlushBlock();

  // Write block_index_buffer_ to page cache
  // current_offset now is starting offset of block section
  ssize_t block_index_size =
      write_file_object_->Append(block_index_buffer_, current_offset_);
  if (block_index_size < 0) {
    throw std::runtime_error("Error when flushing meta section of sstable");
  }

  // Encode extra_buffer and write it to page cache
  EncodeExtraInfo();

  // Update current_offset_
  current_offset_ += block_index_size;

  // current_offset now is starting offset of block section
  ssize_t extra_info_size =
      write_file_object_->Append(extra_buffer_, current_offset_);
  if (extra_info_size < 0) {
    throw std::runtime_error("Error when flushing extra data info to sstable");
  }

  // Update current_offset_
  current_offset_ += extra_info_size;

  // Ensure that data is persisted to disk from page cache
  write_file_object_->Flush();
}

void TableBuilder::EncodeExtraInfo() {
  // Insert total number of entries
  const Byte *const total_block_entries_bytes =
      reinterpret_cast<const Byte *const>(&total_block_entries_);
  extra_buffer_.insert(extra_buffer_.end(), total_block_entries_bytes,
                       total_block_entries_bytes + sizeof(uint64_t));

  // Insert starting offset of meta section
  const Byte *const meta_section_offset_bytes =
      reinterpret_cast<const Byte *const>(&current_offset_);
  extra_buffer_.insert(extra_buffer_.end(), meta_section_offset_bytes,
                       meta_section_offset_bytes + sizeof(uint64_t));

  // Insert size of meta section
  uint64_t block_index_size_u64 =
      static_cast<uint64_t>(block_index_buffer_.size());
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

std::string_view TableBuilder::GetSmallestKey() const {
  return table_smallest_key_;
}

std::string_view TableBuilder::GetLargestKey() const {
  return table_largest_key_;
}

// For testing
BlockBuilder *TableBuilder::GetBlockData() { return block_data_.get(); };

io::WriteOnlyFile *TableBuilder::GetWriteOnlyFileObject() {
  return write_file_object_.get();
}

uint64_t TableBuilder::GetFileSize() const { return current_offset_ + 1; }

uint64_t TableBuilder::GetDataSize() const { return data_size_; }

} // namespace sstable

} // namespace kvs