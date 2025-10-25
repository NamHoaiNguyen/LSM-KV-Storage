#ifndef SSTABLE_TABLE_READER_CACHE_H
#define SSTABLE_TABLE_READER_CACHE_H

#include "common/macros.h"
#include "db/status.h"
#include "sstable/block_index.h"

// libC++
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string_view>
#include <thread>
#include <unordered_map>

namespace kvs {

class ThreadPool;

namespace db {
class DBImpl;
} // namespace db

namespace sstable {

class BlockReaderCache;
class LRUTableItem;
class TableReader;

class TableReaderCache {
public:
  TableReaderCache(const db::DBImpl *db,
                   const kvs::ThreadPool *const thread_pool);

  ~TableReaderCache();

  // No copy allowed
  TableReaderCache(const TableReaderCache &) = delete;
  TableReaderCache &operator=(TableReaderCache &) = delete;

  // Move constructor/assignment
  TableReaderCache(TableReaderCache &&) = delete;
  TableReaderCache &operator=(TableReaderCache &&) = delete;

  db::GetStatus
  GetValue(std::string_view key, TxnId txn_id, SSTId table_id,
           uint64_t file_size,
           const sstable::BlockReaderCache *const block_reader_cache) const;

  std::shared_ptr<LRUTableItem>
  AddNewTableReaderThenGet(SSTId table_id,
                           std::shared_ptr<LRUTableItem> lru_table_item,
                           bool add_then_get) const;

  std::shared_ptr<LRUTableItem> GetLRUTableItem(SSTId table_id) const;

  void AddVictim(SSTId table_id) const;

private:
  // NOT THREAD-SAFE
  void Evict() const;

  void ExecuteBgThread() const;

  const int capacity_;

  mutable std::unordered_map<SSTId, std::shared_ptr<LRUTableItem>>
      table_readers_cache_;

  mutable std::list<SSTId> free_list_;

  mutable std::shared_mutex mutex_;

  mutable std::mutex bg_mutex_;

  mutable std::condition_variable bg_cv_;

  struct ItemCache {
    SSTId table_id;

    std::shared_ptr<LRUTableItem> item;

    bool add_then_get;
  };

  mutable std::queue<std::shared_ptr<LRUTableItem>> victim_queue_;

  mutable std::queue<ItemCache> item_cache_queue_;

  std::atomic<bool> shutdown_;

  const db::DBImpl *const db_;

  const ThreadPool *const thread_pool_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_CACHE_H