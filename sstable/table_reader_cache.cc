#include "sstable/table_reader_cache.h"

#include "sstable/table_reader.h"

namespace kvs {

namespace sstable {

const TableReader *TableReaderCache::GetTableReader(SSTId table_id) const {
  std::shared_lock rlock(mutex_);

  auto table_reader_iterator = table_readers_map_.find(table_id);
  if (table_reader_iterator == table_readers_map_.end()) {
    return nullptr;
  }

  return table_reader_iterator->second.get();
}

void TableReaderCache::AddTableReaderIntoCache(
    SSTId table_id, std::unique_ptr<TableReader> table_reader) {
  std::scoped_lock rwlock(mutex_);

  if (table_readers_map_.find(table_id) != table_readers_map_.end()) {
    return;
  }

  table_readers_map_.insert({table_id, std::move(table_reader)});
}

} // namespace sstable

} // namespace kvs