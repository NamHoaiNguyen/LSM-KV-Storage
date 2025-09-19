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

// NOT THREAD-SAFE. MUST acquire mutex before calling
const TableReader *TableReaderCache::GetTableReader(SSTId table_id) const {
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
    std::shared_lock rlock(mutex_);
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
  // auto new_table_reader =
  //     std::make_unique<TableReader>(std::move(filename), table_id, file_size);
  // if (!new_table_reader->Open()) {
  //   throw std::runtime_error("Can't open SST file to read");
  // }

  auto new_table_reader = CreateAndSetupDataForTableReader(std::move(filename), table_id, file_size);
  if (!new_table_reader) {
    throw std::runtime_error("Can't open SST file to read");
  }

  {
    // Insert table into cache
    std::scoped_lock rwlock(mutex_);
    table_readers_map_.insert({table_id, std::move(new_table_reader)});
  }

  // Search key in new table
  std::shared_lock rlock(mutex_);
  const TableReader *table_reader = GetTableReader(table_id);
  assert(table_reader);
  status = table_reader->SearchKey(key, txn_id, block_reader_cache);
  return status;
}

} // namespace sstable

} // namespace kvs