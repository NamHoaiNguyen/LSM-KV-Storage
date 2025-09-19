#include "sstable/table_reader_cache.h"

#include "db/config.h"
#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"
#include "sstable/table_reader.h"

namespace kvs {

namespace sstable {

TableReaderCache::TableReaderCache(const db::Config *config) : config_(config) {
  assert(config_);
}

const TableReader *TableReaderCache::GetTableReader(SSTId table_id) const {
  std::shared_lock rlock(mutex_);
  auto table_reader_iterator = table_readers_map_.find(table_id);
  if (table_reader_iterator == table_readers_map_.end()) {
    return nullptr;
  }

  return table_reader_iterator->second.get();
}

db::GetStatus TableReaderCache::GetKeyFromTableCache(
    std::string_view key, TxnId txn_id, SSTId table_id, uint64_t file_size,
    const sstable::BlockReaderCache *block_reader_cache) const {
  db::GetStatus status;

  {
    // TODO(namnh) : After getting TableReader from TableReaderCache, lock is
    // released. It means that we need a mechansim to ensure that TableReader is
    // not freed until operation is finished.
    // Temporarily, because of lacking cache eviction mechanism. everything is
    // fine. But when cache eviction algorithm is implemented, ref count for
    // TableReader should be also done too
    const TableReader *table_reader = GetTableReader(table_id);
    if (table_reader) {
      // if table reader had already been in cache
      status = table_reader->SearchKey(key, txn_id, block_reader_cache);
      return status;
    }
  }

  // if table hadn't been in cache, create new table and load into cache
  std::string filename =
      config_->GetSavedDataPath() + std::to_string(table_id) + ".sst";

  // Create new table reader
  auto new_table_reader = CreateAndSetupDataForTableReader(std::move(filename),
                                                           table_id, file_size);
  if (!new_table_reader) {
    throw std::runtime_error("Can't open SST file to read");
  }

  {
    // Insert table into cache
    std::scoped_lock rwlock(mutex_);
    table_readers_map_.insert({table_id, std::move(new_table_reader)});
  }

  // Search key in new table
  // TODO(namnh) : Same as above
  const TableReader *table_reader = GetTableReader(table_id);
  assert(table_reader);
  status = table_reader->SearchKey(key, txn_id, block_reader_cache);
  return status;
}

} // namespace sstable

} // namespace kvs