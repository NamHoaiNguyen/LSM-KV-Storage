#ifndef SSTABLE_BLOCK_READER_CACHE_H
#define SSTABLE_BLOCK_READER_CACHE_H

#include "common/macros.h"
#include "db/status.h"
#include "sstable/block_index.h"

// libC++
#include <cassert>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>

namespace kvs {

class ThreadPool;

namespace sstable {

class BlockReader;
class LRUBlockItem;
class LRUTableItem;
class TableReaderCache;
class TableReader;

class BlockReaderCache {
public:
  BlockReaderCache(int capacity, kvs::ThreadPool *thread_pool);

  ~BlockReaderCache() = default;

  // No copy allowed
  BlockReaderCache(const BlockReaderCache &) = delete;
  BlockReaderCache &operator=(BlockReaderCache &) = delete;

  // Move constructor/assignment
  BlockReaderCache(BlockReaderCache &&) = delete;
  BlockReaderCache &operator=(BlockReaderCache &&) = delete;

  // const LRUBlockItem *
  std::shared_ptr<LRUBlockItem>
  GetLRUBlockItem(std::pair<SSTId, BlockOffset> lock_info) const;

  // const LRUBlockItem *
  // std::pair<const LRUBlockItem *, std::unique_ptr<LRUBlockItem>>
  std::pair<std::shared_ptr<LRUBlockItem>, std::shared_ptr<LRUBlockItem>>
  AddNewBlockReaderThenGet(std::pair<SSTId, BlockOffset> block_info,
                           //  std::unique_ptr<LRUBlockItem> block_reader,
                           std::shared_ptr<LRUBlockItem> block_reader,
                           bool add_then_get) const;

  db::GetStatus GetKeyFromBlockCache(std::string_view key, TxnId txn_id,
                                     std::pair<SSTId, BlockOffset> block_info,
                                     uint64_t block_size,
                                     const TableReader *table_reader) const;

  void AddVictim(std::pair<SSTId, BlockOffset> block_info) const;

private:
  // NOT THREAD-SAFE
  bool Evict() const;

  bool CanCreateNewBlockReader() const;

  // Custom hash for pair<int, int>
  struct pair_hash {
    size_t operator()(const std::pair<SSTId, BlockOffset> &p) const noexcept {
      // Combine the hashes of both sst id and block offset
      return std::hash<uint64_t>()(p.first) ^
             (std::hash<uint64_t>()(p.second) << 1);
    }
  };

  struct pair_equal {
    bool operator()(const std::pair<SSTId, BlockOffset> &a,
                    const std::pair<SSTId, BlockOffset> &b) const noexcept {
      return a.first == b.first && a.second == b.second;
    }
  };

  // Each key of block reader item in block reader cache is the combination
  // of table id and block offset
  // mutable std::unordered_map<std::pair<SSTId, BlockOffset>,
  //                            std::unique_ptr<LRUBlockItem>, pair_hash,
  //                            pair_equal>
  //     block_reader_cache_;

  mutable std::unordered_map<std::pair<SSTId, BlockOffset>,
                             std::shared_ptr<LRUBlockItem>, pair_hash,
                             pair_equal>
      block_reader_cache_;

  const int capacity_;

  mutable std::list<std::pair<SSTId, BlockOffset>> free_list_;

  mutable std::shared_mutex mutex_;

  kvs::ThreadPool *thread_pool_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_READER_CACHE_H