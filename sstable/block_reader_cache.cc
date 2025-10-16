#include "sstable/block_reader_cache.h"

#include "common/thread_pool.h"
#include "sstable/block_reader.h"
#include "sstable/lru_block_item.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"

namespace kvs {

namespace sstable {

BlockReaderCache::BlockReaderCache(kvs::ThreadPool *thread_pool)
    : capacity_(100000), thread_pool_(thread_pool) {}

const LRUBlockItem *BlockReaderCache::GetBlockReader(
    std::pair<SSTId, BlockOffset> block_info) const {
  std::shared_lock rlock(mutex_);
  auto iterator = block_reader_cache_.find(block_info);
  if (iterator == block_reader_cache_.end()) {
    return nullptr;
  }

  // Increase ref count
  iterator->second->IncRef();

  return iterator->second.get();
}

std::pair<const LRUBlockItem *, std::unique_ptr<LRUBlockItem>>
BlockReaderCache::AddNewBlockReaderThenGet(
    std::pair<SSTId, BlockOffset> block_info,
    std::unique_ptr<LRUBlockItem> lru_block_item, bool add_then_get) const {
  // Insert new block reader into cache
  std::scoped_lock rwlock(mutex_);

  if (block_reader_cache_.size() >= capacity_) {
    if (!Evict()) {
      return {nullptr, std::move(lru_block_item)};
    }
  }

  bool success =
      block_reader_cache_.insert({block_info, std::move(lru_block_item)})
          .second;

  // Get block reader that MAYBE inserted
  auto iterator = block_reader_cache_.find(block_info);

  // Increase ref count
  if (success) {
    // First time
    iterator->second->ref_count_.fetch_add(1);
  }

  if (add_then_get) {
    // Increase ref count if need to get
    iterator->second->ref_count_.fetch_add(1);
  }

  if (iterator->second->ref_count_.load() <= 1) {
    // This BlockReader should be put in free_list_
    free_list_.push_back(block_info);
  }

  return {iterator->second.get(), nullptr};
}

// NOT THREAD-SAFE
bool BlockReaderCache::Evict() const {
  if (free_list_.empty()) {
    return false;
  }

  std::pair<SSTId, BlockOffset> block_info = free_list_.front();
  assert(!free_list_.empty());
  free_list_.pop_front();
  auto iterator = block_reader_cache_.find(block_info);

  while (iterator != block_reader_cache_.end() &&
         iterator->second->ref_count_ > 1 && !free_list_.empty()) {
    block_info = free_list_.front();
    iterator = block_reader_cache_.find(block_info);
    free_list_.pop_front();
  }

  // Erase from cache
  auto deleted = block_reader_cache_.erase(block_info);
  return (deleted == 0) ? Evict() : true;
}

void BlockReaderCache::AddVictim(
    std::pair<SSTId, BlockOffset> block_info) const {
  std::scoped_lock rwlock(mutex_);
  free_list_.push_back(block_info);
}

bool BlockReaderCache::CanCreateNewBlockReader() const {
  std::shared_lock rlock(mutex_);
  return block_reader_cache_.size() >= capacity_;
}

db::GetStatus
BlockReaderCache::GetKeyFromBlockCache(std::string_view key, TxnId txn_id,
                                       std::pair<SSTId, BlockOffset> block_info,
                                       uint64_t block_size,
                                       const TableReader *table_reader) const {
  assert(table_reader);
  db::GetStatus status;

  const LRUBlockItem *block_reader = GetBlockReader(block_info);
  if (block_reader && block_reader->block_reader_) {
    // if tablereader had already been in cache
    assert(block_reader->ref_count_ >= 2);
    status = block_reader->block_reader_->SearchKey(key, txn_id);
    thread_pool_->Enqueue(&LRUBlockItem::Unref, block_reader);
    return status;
  }

  // Create new tablereader
  auto new_block_reader = table_reader->CreateAndSetupDataForBlockReader(
      block_info.second, block_size);
  if (!new_block_reader) {
    status.type = db::ValueType::kTooManyOpenFiles;
    return status;
  }

  auto new_lru_block_item = std::make_unique<LRUBlockItem>(
      block_info, std::move(new_block_reader), this);

  status = new_lru_block_item->GetBlockReader()->SearchKey(key, txn_id);

  AddNewBlockReaderThenGet(block_info, std::move(new_lru_block_item),
                           false /*need_to_get*/);

  // thread_pool_->Enqueue(
  //     [this, block_info, table_reader,
  //      new_lru_block_item = std::move(new_lru_block_item)]() mutable {
  //       AddNewBlockReaderThenGet(block_info, std::move(new_lru_block_item),
  //                                false /*need_to_get*/);
  //     });

  return status;
}

} // namespace sstable

} // namespace kvs