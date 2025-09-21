#include "sstable/block_reader_cache.h"

#include "sstable/block_reader.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"

namespace kvs {

namespace sstable {

const BlockReader *BlockReaderCache::GetBlockReader(
    std::pair<SSTId, BlockOffset> block_info) const {
  std::shared_lock rlock(mutex_);
  auto block_reader_iterator = block_reader_cache_.find(block_info);
  if (block_reader_iterator == block_reader_cache_.end()) {
    return nullptr;
  }

  return block_reader_iterator->second.get();
}

void BlockReaderCache::AddNewBlockReader(
    std::pair<SSTId, BlockOffset> block_info,
    std::unique_ptr<BlockReader> block_reader) const {
  std::scoped_lock rwlock(mutex_);
  block_reader_cache_.insert(
      std::make_pair(block_info, std::move(block_reader)));
}

db::GetStatus
BlockReaderCache::GetKeyFromBlockCache(std::string_view key, TxnId txn_id,
                                       std::pair<SSTId, BlockOffset> block_info,
                                       uint64_t block_size,
                                       const TableReader *table_reader) const {
  assert(table_reader);
  db::GetStatus status;

  // TODO(namnh) : After getting BlockReader from BlockReaderCache, lock is
  // released. It means that we need a mechansim to ensure that TableReader is
  // not freed until operation is finished.
  // Temporarily, because of lacking cache eviction mechanism. everything is
  // fine. But when cache eviction algorithm is implemented, ref count for
  // BlockReader should be also done too
  const BlockReader *block_reader = GetBlockReader(block_info);
  if (block_reader) {
    // if tablereader had already been in cache
    status = block_reader->SearchKey(key, txn_id);
    return status;
  }

  // Create new tablereader
  auto new_block_reader = table_reader->CreateAndSetupDataForBlockReader(
      block_info.second, block_size);

  status = new_block_reader->SearchKey(key, txn_id);

  // Insert new blockreader into cache
  AddNewBlockReader(block_info, std::move(new_block_reader));

  return status;
}

} // namespace sstable

} // namespace kvs