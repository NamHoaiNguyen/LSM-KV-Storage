#ifndef SSTABLE_TABLE_READER_CACHE_H
#define SSTABLE_TABLE_READER_CACHE_H

#include "common/macros.h"
#include "sstable/block_index.h"

// libC++
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace kvs {

namespace sstable {

class TableReader;

class TableReaderCache {
public:
  TableReaderCache() = default;

  ~TableReaderCache() = default;

  // No copy allowed
  TableReaderCache(const TableReaderCache &) = delete;
  TableReaderCache &operator=(TableReaderCache &) = delete;

  // Move constructor/assignment
  TableReaderCache(TableReaderCache &&) = default;
  TableReaderCache &operator=(TableReaderCache &&) = default;

  const TableReader *GetTableReader(SSTId table_id) const;

  void AddTableReaderIntoCache(SSTId table_id, std::unique_ptr<TableReader>);

private:
  std::unordered_map<SSTId, std::unique_ptr<TableReader>> table_readers_map_;

  mutable std::shared_mutex mutex_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_CACHE_H