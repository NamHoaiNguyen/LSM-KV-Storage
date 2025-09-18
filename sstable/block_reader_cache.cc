#include "sstable/block_reader_cache.h"

#include "sstable/block_reader.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"

namespace kvs {

namespace sstable {

BlockReaderCache::BlockReaderCache(const TableReaderCache *table_reader_cache)
    : table_reader_cache_(table_reader_cache) {
  assert(table_reader_cache_);
}

// NOT THREAD-SAFE. MUST acquire mutex before calling
const BlockReader *BlockReaderCache::GetBlockReader(
    std::pair<SSTId, BlockOffset> block_info) const {
  auto block_reader_iterator = block_reader_cache_.find(block_info);
  if (block_reader_iterator == block_reader_cache_.end()) {
    return nullptr;
  }

  return block_reader_iterator->second.get();
}

db::GetStatus
BlockReaderCache::GetKeyFromBlockCache(std::string_view key, TxnId txn_id,
                                       std::pair<SSTId, BlockOffset> block_info,
                                       uint64_t block_size) const {

  db::GetStatus status;

  {
    std::shared_lock rlock(mutex_);
    const BlockReader *block_reader = GetBlockReader(block_info);
    if (block_reader) {
      // if table reader had already been in cache
      status = block_reader->SearchKey(key, txn_id);
      return status;
    }
  }

  const TableReader *table_reader =
      table_reader_cache_->GetTableReader(block_info.first);
  assert(table_reader);

  // Create new table reader
  auto new_block_reader = std::make_unique<BlockReader>(
      table_reader->GetReadFileObject(), block_size);
  // Immediately fetch data from block into memory
  if (!new_block_reader->FetchBlockData(block_info.second)) {
    return status;
  }

  {
    // Insert new block reader into cache
    std::scoped_lock rwlock(mutex_);
    block_reader_cache_.insert(
        std::make_pair(block_info, std::move(new_block_reader)));
  }

  // Search key in new block reader
  std::shared_lock rlock(mutex_);
  const BlockReader *block_reader = GetBlockReader(block_info);
  assert(block_reader);

  status = block_reader->SearchKey(key, txn_id);
  return status;
}

} // namespace sstable

} // namespace kvs