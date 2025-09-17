#include "sstable/table_reader_cache.h"

#include "sstable/table_reader.h"

namespace kvs {

namespace sstable {

TableReaderCache::TableReaderCache(const Config* config)
    : config_(config) {}

const TableReader *TableReaderCache::GetTableReader(SSTId table_id) const {
  std::shared_lock rlock(mutex_);

  auto table_reader_iterator = table_readers_map_.find(table_id);
  if (table_reader_iterator == table_readers_map_.end()) {
    return nullptr;
  }

  return table_reader_iterator->second.get();
}

GetStatus TableReaderCache::GetKeyFromTableCache(std::string_view key,
                                                 TxnId txn_id,
                                                 SSTId table_id) const {
  GetStatus status;

  const TableReader* table_reader = GetTableReader(table_id);
  if (!table_reader) {
    // create new table and load into cache
    std::string filename =
        config_->GetSavedDataPath() + std::to_string(table_id) + ".sst";

    // Create new table reader
    auto table_reader = std::make_unique<TableReader>(std::move(filename), file_size);
    if (!table_reader->Open()) {
      throw std::runtime_error("Can't open SST file to read");
    }

    // Search key in new table
    status = table_reader->GetKey(key, txn_id);

    // Insert table into cache
    {
      std::scoped_lock rwlock(mutex_);
      table_readers_map_.insert({table_id, std::move(table_reader)});
    }

    return status;
  }

  // if table reader had already been in cache
  status = table_reader->GetKey(key, txn_id);
  return status;
}

} // namespace sstable

} // namespace kvs