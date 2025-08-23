#include "sstable/table_builder.h"

#include "db/memtable_iterator.h"
#include "io/base_file.h"
#include "io/linux_file.h"
#include "sstable/block_builder.h"

namespace kvs {

TableBuilder::TableBuilder(std::string &&filename)
    : file_object_(std::make_unique<LinuxAccessFile>(std::move(filename))),
      block_data_(std::make_shared<BlockBuilder>()), block_data_size_(0),
      index_block_(std::make_shared<BlockBuilder>()), block_index_size_(0), 
      current_offset_(0) {}

void TableBuilder::AddEntry(std::string_view key, std::string_view value,
                     TxnId txn_id) {
  block_data_->AddEntry(key, value, txn_id);

  // const size_t block_size = block_data_->GetBlockSize();
  block_data_size_ += sizeof(uint32_t) + key.size() + sizeof(uint32_t) +
                      value.size() + sizeof(TxnId) + 2 * sizeof(uint64_t);
  // TODO(namnh) : For debug
  if (block_data_size_ >= 50) {
    FlushBlock();
  }
}

void TableBuilder::FlushBlock() {
  const uint64_t block_starting_offset = current_offset_;

  std::span<const Byte> data_buffer = block_data_->GetDataView();
  file_object_->Append(data_buffer, current_offset_);
  current_offset_ += data_buffer.size();

  std::span<const Byte> offset_buffer = block_data_->GetOffsetView();
  file_object_->Append(offset_buffer, current_offset_);
  current_offset_ += offset_buffer.size();

  file_object_->Flush();

  // Build MetaEntry format (block_meta)

}

void TableBuilder::WriteBlock() {}

} // namespace kvs