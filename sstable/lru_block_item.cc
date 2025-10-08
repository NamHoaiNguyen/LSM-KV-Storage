#include "sstable/lru_block_item.h"

#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"

namespace kvs {

namespace sstable {

LRUBlockItem::LRUBlockItem(std::pair<SSTId, BlockOffset> block_info,
                           std::unique_ptr<BlockReader> block_reader,
                           const BlockReaderCache *cache)
    : ref_count_(0), table_id_(block_info.first),
      block_offset_(block_info.second), block_reader_(std::move(block_reader)),
      cache_(cache) {}

void LRUBlockItem::Unref() const {
  if (ref_count_.fetch_sub(1) == 1) {
    cache_->AddVictim({table_id_, block_offset_});
  }
}

const BlockReader *LRUBlockItem::GetBlockReader() const {
  return block_reader_.get();
}

} // namespace sstable

} // namespace kvs