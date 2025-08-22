#include "sstable/table.h"

#include "db/memtable_iterator.h"
#include "io/base_file.h"
#include "io/linux_file.h"
#include "sstable/block.h"

namespace kvs {

Table::Table(std::string &&filename)
    : file_object_(std::make_unique<LinuxAccessFile>(std::move(filename))),
      data_block_(std::make_shared<Block>()),
      index_block_(std::make_shared<Block>()), current_offset_(0) {}

void Table::AddEntry(std::string_view key, std::string_view value,
                     TxnId txn_id) {
  data_block_->AddEntry(key, value, txn_id);

  const size_t block_size = data_block_->GetBlockSize();
  if (block_size >= 50) {
    FlushBlock();
  }
}

void Table::FlushBlock() {
  std::span<const Byte> data_buffer = data_block_->GetDataView();
  file_object_->Append(data_buffer, current_offset_);

  current_offset_ += data_buffer.size();

  std::span<const Byte> offset_buffer = data_block_->GetOffsetView();
  file_object_->Append(offset_buffer, current_offset_);

  current_offset_ += offset_buffer.size();

  file_object_->Flush();

  // current_offset_ += data_block_->GetBlockSize();
}

void Table::WriteBlock() {}

} // namespace kvs