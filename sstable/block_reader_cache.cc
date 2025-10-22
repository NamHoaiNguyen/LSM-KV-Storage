#include "sstable/block_reader_cache.h"

#include "common/thread_pool.h"
#include "sstable/block_reader.h"
#include "sstable/lru_block_item.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"

namespace kvs {

namespace sstable {

BlockReaderCache::BlockReaderCache(int capacity, kvs::ThreadPool *thread_pool)
    : capacity_(10000), thread_pool_(thread_pool) {
  thread_pool_->Enqueue(&BlockReaderCache::UnrefThread, this);
  thread_pool_->Enqueue(&BlockReaderCache::AddNewItemThread, this);
}

BlockReaderCache::~BlockReaderCache() {
  shutdown_.store(true);

  unref_cv_.notify_one();

  item_cv_.notify_one();
}

std::shared_ptr<LRUBlockItem> BlockReaderCache::GetLRUBlockItem(
    std::pair<SSTId, BlockOffset> block_info) const {
  std::shared_lock rlock(mutex_);
  auto iterator = block_reader_cache_.find(block_info);
  if (iterator == block_reader_cache_.end()) {
    return nullptr;
  }

  // Increase ref count
  iterator->second->IncRef();

  return iterator->second;
}

std::shared_ptr<LRUBlockItem> BlockReaderCache::AddNewBlockReaderThenGet(
    std::pair<SSTId, BlockOffset> block_info,
    std::shared_ptr<LRUBlockItem> lru_block_item, bool add_then_get) const {

  // Insert new block reader into cache
  std::scoped_lock rwlock(mutex_);

  if (block_reader_cache_.size() >= capacity_) {
    if (!Evict()) {
      return nullptr;
    }
  }

  bool success =
      block_reader_cache_.insert({block_info, std::move(lru_block_item)})
          .second;

  // Get block reader that MAYBE inserted
  auto iterator = block_reader_cache_.find(block_info);

  if (add_then_get) {
    // Increase ref count if need to get
    iterator->second->IncRef();
  }

  if (iterator->second->GetRefCount() < 1) {
    // This BlockReader should be put in free_list_
    free_list_.push_back(block_info);
  }

  return iterator->second;
}

// NOT THREAD-SAFE
bool BlockReaderCache::Evict() const {
  if (free_list_.empty()) {
    return false;
  }

  if (block_reader_cache_.empty()) {
    return false;
  }

  std::pair<SSTId, BlockOffset> block_info = free_list_.front();
  assert(!free_list_.empty());
  free_list_.pop_front();
  auto iterator = block_reader_cache_.find(block_info);

  while (iterator != block_reader_cache_.end() &&
         iterator->second->GetRefCount() >= 1 && !free_list_.empty()) {
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

void BlockReaderCache::AddNewItemThread() const {
  while (!shutdown_) {
    {
      std::unique_lock rwlock(item_mutex_);
      item_cv_.wait(rwlock, [this]() {
        return this->shutdown_ || !this->new_item_queue_.empty();
      });

      if (this->shutdown_) {
        return;
      }

      while (!new_item_queue_.empty()) {
        ItemQueue item = new_item_queue_.front();
        new_item_queue_.pop();

        AddNewBlockReaderThenGet(item.info, item.item, item.add_then_get);
      }
    }
  }
}

void BlockReaderCache::UnrefThread() const {
  while (!shutdown_) {
    {
      std::unique_lock rwlock(unref_q_mutex_);
      unref_cv_.wait(rwlock, [this]() {
        return this->shutdown_ || !this->unref_queue_.empty();
      });

      if (this->shutdown_) {
        return;
      }

      while (!unref_queue_.empty()) {
        std::shared_ptr<LRUBlockItem> item = unref_queue_.front();
        unref_queue_.pop();

        item->Unref();
      }
    }
  }
}

db::GetStatus
BlockReaderCache::GetValue(std::string_view key, TxnId txn_id,
                           std::pair<SSTId, BlockOffset> block_info,
                           uint64_t block_size,
                           const TableReader *table_reader) const {
  assert(table_reader);
  db::GetStatus status;

  std::shared_ptr<LRUBlockItem> block_reader = GetLRUBlockItem(block_info);
  if (block_reader && block_reader->GetBlockReader()) {
    // if tablereader had already been in cache
    // assert(block_reader->ref_count_ >= 2);
    status = block_reader->GetBlockReader()->GetValue(key, txn_id);
    // TODO(namnh) : This can improve performance, but increase CPU usage
    // significantly
    // thread_pool_->Enqueue(&LRUBlockItem::Unref, block_reader);
    {
      std::scoped_lock rwlock(unref_q_mutex_);
      unref_queue_.push(block_reader);
      unref_cv_.notify_one();
    }
    return status;
  }

  // Create new tablereader
  auto new_block_reader = table_reader->CreateAndSetupDataForBlockReader(
      block_info.second, block_size);
  if (!new_block_reader) {
    status.type = db::ValueType::kTooManyOpenFiles;
    return status;
  }

  auto new_lru_block_item = std::make_shared<LRUBlockItem>(
      block_info, std::move(new_block_reader), this);

  status = new_lru_block_item->GetBlockReader()->GetValue(key, txn_id);

  // AddNewBlockReaderThenGet(block_info, new_lru_block_item,
  //                          false /*need_to_get*/);
  {
    std::scoped_lock rwlock(item_mutex_);
    new_item_queue_.push(
        {block_info, new_lru_block_item, false /*need_to_get*/});
    item_cv_.notify_one();
  }

  return status;
}

} // namespace sstable

} // namespace kvs