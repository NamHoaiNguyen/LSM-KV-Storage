#ifndef SSTABLE_TABLE_READER_CACHE_H
#define SSTABLE_TABLE_READER_CACHE_H

#include "common/macros.h"
#include "db/status.h"
#include "sstable/block_index.h"

// libC++
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>

namespace kvs {

namespace db {
class Config;
}

namespace sstable {

class BlockReaderCache;
class TableReader;

class TableReaderCache {
public:
  explicit TableReaderCache(const db::Config *config);

  ~TableReaderCache() = default;

  // No copy allowed
  TableReaderCache(const TableReaderCache &) = delete;
  TableReaderCache &operator=(TableReaderCache &) = delete;

  // Move constructor/assignment
  TableReaderCache(TableReaderCache &&) = default;
  TableReaderCache &operator=(TableReaderCache &&) = default;

  db::GetStatus GetKeyFromTableCache(
      std::string_view key, TxnId txn_id, SSTId table_id, uint64_t file_size,
      const sstable::BlockReaderCache *block_reader_cache) const;

  void AddNewTableReader(SSTId table_id,
                         std::unique_ptr<TableReader> table_reader) const;

  const TableReader *GetTableReader(SSTId table_id) const;

private:
  const db::Config *config_;

  mutable std::unordered_map<SSTId, std::unique_ptr<TableReader>>
      table_readers_cache_;

  mutable std::shared_mutex mutex_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_CACHE_H