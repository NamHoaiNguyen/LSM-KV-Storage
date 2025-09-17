#ifndef SSTABLE_TABLE_READER_CACHE_H
#define SSTABLE_TABLE_READER_CACHE_H

#include "common/macros.h"
#include "sstable/block_index.h"

// libC++
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>

namespace kvs {

namespace sstable {

class TableReader;

class TableReaderCache {
public:
  explicit TableReaderCache(const Config* config);

  ~TableReaderCache() = default;

  // No copy allowed
  TableReaderCache(const TableReaderCache &) = delete;
  TableReaderCache &operator=(TableReaderCache &) = delete;

  // Move constructor/assignment
  TableReaderCache(TableReaderCache &&) = default;
  TableReaderCache &operator=(TableReaderCache &&) = default;

  GetStatus GetKeyFromTableCache(std::string_view key,
                                 TxnId txn_id, SSTId table_id) const;

private:
  const TableReader *GetTableReader(SSTId table_id) const;

  const Config* config_;

  std::unique_ptr<BlockC

  mutable std::unordered_map<SSTId, std::unique_ptr<TableReader>> table_readers_map_;

  mutable std::shared_mutex mutex_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_CACHE_H