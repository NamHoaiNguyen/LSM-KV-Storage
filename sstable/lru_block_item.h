#ifndef COMMON_LRU_BLOCK_ITEM_H
#define COMMON_LRU_BLOCK_ITEM_H

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

class BlockReaderCache;
class BlockReader;

class LRUBlockItem {
public:
  LRUBlockItem(std::pair<SSTId, BlockOffset> block_info,
               std::unique_ptr<BlockReader> block_reader,
               const BlockReaderCache *cache_);

  ~LRUBlockItem() = default;

  // No copy allowed
  LRUBlockItem(const LRUBlockItem &) = delete;
  LRUBlockItem &operator=(LRUBlockItem &) = delete;

  // Move constructor/assignment
  LRUBlockItem(LRUBlockItem &&) = default;
  LRUBlockItem &operator=(LRUBlockItem &&) = default;

  void IncRef() const;

  // It must be called each time an operation is finished
  void Unref() const;

  const BlockReader *GetBlockReader() const;

  uint64_t GetRefCount() const;

  friend class BlockReaderCache;

private:
  mutable std::atomic<int64_t> ref_count_;

  SSTId table_id_;

  BlockOffset block_offset_;

  std::unique_ptr<BlockReader> block_reader_;

  const BlockReaderCache *cache_;
};

} // namespace sstable

} // namespace kvs

#endif // COMMON_LRU_CACHE_ITEM_H