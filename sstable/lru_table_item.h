#ifndef COMMON_LRU_TABLE_ITEM_H
#define COMMON_LRU_TABLE_ITEM_H

#include "common/macros.h"

// libC++
#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace kvs {

namespace sstable {

class TableReaderCache;
class TableReader;

class LRUTableItem {
public:
  LRUTableItem(SSTId table_id, std::unique_ptr<TableReader> table_reader,
               const TableReaderCache *cache_);

  ~LRUTableItem() = default;

  // No copy allowed
  LRUTableItem(const LRUTableItem &) = delete;
  LRUTableItem &operator=(LRUTableItem &) = delete;

  // Move constructor/assignment
  LRUTableItem(LRUTableItem &&) = default;
  LRUTableItem &operator=(LRUTableItem &&) = default;

  void IncRef() const;

  // It must be called each time an operation is finished
  void Unref() const;

  const TableReader *GetTableReader() const;

  friend class TableReaderCache;

private:
  mutable std::atomic<uint64_t> ref_count_;

  SSTId table_id_;

  std::unique_ptr<TableReader> table_reader_;

  const TableReaderCache *cache_;
};

} // namespace sstable

} // namespace kvs

#endif // COMMON_LRU_TABLE_ITEM_H