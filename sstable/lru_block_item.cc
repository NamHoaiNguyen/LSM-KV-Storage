#include "sstable/lru_block_item.h"

#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"

#include <iostream>

namespace kvs {

namespace sstable {

LRUBlockItem::LRUBlockItem(std::pair<SSTId, BlockOffset> block_info,
                           std::unique_ptr<BlockReader> block_reader,
                           const BlockReaderCache *cache)
    : ref_count_(0), table_id_(block_info.first),
      block_offset_(block_info.second), block_reader_(std::move(block_reader)),
      cache_(cache) {}

void LRUBlockItem::IncRef() const { ref_count_.fetch_add(1); }

void LRUBlockItem::Unref() const {
  if (ref_count_.load() < 0) {
    std::cout << "namnh BUGG!!! refcount of LRUBlockItem MUST >= 0 "
              << std::endl;
  }

  // if (ref_count_.fetch_sub(1) == 1) {
  if (ref_count_.fetch_sub(1) <= 2) {
    cache_->AddVictim({table_id_, block_offset_});
  }
}

uint64_t LRUBlockItem::GetRefCount() const { return ref_count_.load(); }

const BlockReader *LRUBlockItem::GetBlockReader() const {
  return block_reader_.get();
}

} // namespace sstable

} // namespace kvs