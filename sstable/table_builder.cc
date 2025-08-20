#include "sstable/table_builder.h"

#include "db/memtable_iterator.h"
#include "io/linux_file.h"

namespace kvs {

TableBuilder::TableBuilder(std::string&& filename)
    : file_object_(std::make_unique<LinuxAccessFile>(std::move(filename))),
      data_block_(std::make_shared<BlockBuilder>()),
      index_block_(std::make_shared<BlockBuilder,
      iterator_(std::make_unique<MemTableIterator>()) {}

void TableBuilder::Add(std::string_view key, std::string_view value) {
  data_block_->Add(key, value);
  
  const size_t block_size = data_block_->GetBlockSize();
  if (block_size >= kDefaultBufferSize) {
    Flush();
  }
}

void TableBuilder::FlushBlock() {
  file_object_->Flush();
}

void TableBuilder::WriteBlock() {}

} // namespace kvs