#include "sstable/table.h"

#include "db/memtable_iterator.h"
#include "io/base_file.h"
#include "io/linux_file.h"
#include "sstable/block_index.h"
#include "sstable/block.h"

namespace kvs {

Table::Table(std::string &&filename)
    : file_object_(std::make_unique<LinuxAccessFile>(std::move(filename))),
      block_data_(std::make_shared<Block>()), block_data_size_(0),
      index_block_(std::make_shared<Block>()), block_index_size_(0), 
      current_offset_(0) {}

void Table::AddEntry(std::string_view key, std::string_view value,
                     TxnId txn_id) {
  if (block_data_->GetBlockSize() == 0) {
    block_first_key_(key);
  }
  block_data_->AddEntry(key, value, txn_id);

  // TODO(namnh) : For debug
  if (block_size >= 50) {
    block_last_key_(key);
    FlushBlock();
  }
}

void Table::FlushBlock() {
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
  file_object_->Flush();

  // Build MetaEntry format (block_meta)
  block_index_->AddEntry(block_first_key_, block_last_key_,
                         block_starting_offset,
                         current_offset_ - block_starting_offset);

  // Reset block data
  block_data_->Reset();
}

void Table::Finish() {
  // Write block_index_ to page cache
  std::span<const Byte> block_index_buffer = block_index_->GetBufferView();
  file_object_->Append(block_index_buffer, current_offset_);

  // Ensure that data is persisted to disk from page cache
  file_object_->Flush();
}

void Table::WriteBlock() {}

} // namespace kvs