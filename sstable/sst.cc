#include "sstable/sst.h"

#include "db/config.h"
#include "db/memtable_iterator.h"
#include "io/base_file.h"
#include "io/linux_file.h"
#include "sstable/block.h"
#include "sstable/block_index.h"

// libC++
#include <cassert>

namespace kvs {

namespace sstable {

Table::Table(std::string &&filename, const db::Config *config)
    : file_object_(
          std::make_unique<io::LinuxWriteOnlyFile>(std::move(filename))),
      block_data_(std::make_unique<Block>()),
      block_index_(std::make_unique<BlockIndex>()), current_offset_(0),
      min_txnid_(UINT64_MAX), max_txnid_(0), config_(config) {}

void Table::AddEntry(std::string_view key, std::string_view value, TxnId txn_id,
                     db::ValueType value_type) {
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
  assert(file_object_);

  // Starting offset of block
  const uint64_t block_starting_offset = current_offset_;

  // Flush data part of data block to disk
  std::span<const Byte> data_buffer = block_data_->GetDataView();
  file_object_->Append(data_buffer, current_offset_);
  current_offset_ += data_buffer.size();

  // Flush metadata part of data block to disk
  std::span<const Byte> offset_buffer = block_data_->GetOffsetView();
  file_object_->Append(offset_buffer, current_offset_);
  current_offset_ += offset_buffer.size();

  // Ensure that data is persisted to disk from page cache
  // TODO(namnh, IMPORTANCE) : Do we need to do that right now? it significantly
  // degrades performance
  // file_object_->Flush();

  // Build MetaEntry format (block_meta)
  block_index_->AddEntry(block_smallest_key_, block_largest_key_,
                         block_starting_offset,
                         current_offset_ - block_starting_offset);

  // Reset block data
  block_data_->Reset();
}

void Table::Finish() {
  // Flush remaining data to
  FlushBlock();

  // Write block_index_ to page cache
  std::span<const Byte> block_index_buffer = block_index_->GetBufferView();
  // current_offset now is starting offset of block section
  ssize_t block_index_size =
      file_object_->Append(block_index_buffer, current_offset_);
  if (block_index_size < 0) {
    throw std::runtime_error("Error when flushing meta section of sstable");
  }

  EncodeExtraInfo();

  // Ensure that data is persisted to disk from page cache
  if (file_object_->Flush()) {
    // All data in SST is persisted to disk. Free file object
    // to not allow any writing
    file_object_.reset();
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
      static_cast<uint64_t>(block_index_->GetBlockIndexSize());
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
  if (!file_object_) {
    return false;
  }

  return file_object_->Open();
}

// For testing
Block *Table::GetBlockData() { return block_data_.get(); };

BlockIndex *Table::GetBlockIndexData() { return block_index_.get(); };

io::WriteOnlyFile *Table::GetWriteOnlyFileObject() {
  return file_object_.get();
}

} // namespace sstable

} // namespace kvs