#include "sstable/block_reader_cache.h"

#include "sstable/table_reader_cache.h"
#include "sstable/table_reader.h"

namespace kvs {

namespace sstable {

BlockReaderCache::BlockReaderCache(const TableReaderCache* table_reader_cache)
    : table_reader_cache_(table_reader_cache) {}

const BlockReader *BlockReaderCache::GetBlockReader(std::pair<SSTId, BlockOffset> block_info) const {
  std::shared_lock rlock(mutex_);

  auto block_reader_iterator = block_readers_map_.find(block_info);
  if (block_reader_iterator == block_readers_map_.end()) {
    return nullptr;
  }

  return block_reader_iterator->second.get();
}

GetStatus BlockReaderCache::GetKeyFromBlockCache(std::string_view key,
                                                 TxnId txn_id,
                                                 std::pair<SSTId, BlockOffset> block_info) const {
  GetStatus status;

  const BlockReader* block_reader = GetBlockReader(block_info);
  if (!block_reader) {
    // Get table reader from table reader cache
    const TableReader* table_reader =
        table_reader_cache_->GetTableReader(block_info.first);
    assert(table_reader);

    // Create new table reader
    auto block_reader = 
        std::make_unique<BlockReader>(table_reader->GetReadFileObject(),
                                      table_reader->GetFileSize());

    // Search key in new table
    status = block_reader->GetKey(key, txn_id);

    // Insert table into cache
    {
      std::scoped_lock rwlock(mutex_);
      block_readers_map_.insert({block_info, std::move(table_reader)});
    }

    return status;
  }

  // if table reader had already been in cache
  status = block_reader->GetKey(key, txn_id);
  return status;
}

} // namespace sstable

} // namespace kvs