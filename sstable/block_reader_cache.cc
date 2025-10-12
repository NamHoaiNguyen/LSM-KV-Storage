#include "sstable/block_reader_cache.h"

#include "common/thread_pool.h"
#include "sstable/block_reader.h"
#include "sstable/lru_block_item.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"

#include <iostream>

namespace kvs {

namespace sstable {

BlockReaderCache::BlockReaderCache(kvs::ThreadPool *thread_pool)
    : capacity_(100000), shutdown_(false), deleted_(false), batch_(0),
      thread_pool_(thread_pool) {
  thread_pool_->Enqueue(&BlockReaderCache::EvictV2, this);
}

BlockReaderCache::~BlockReaderCache() {
  shutdown_ = true;

  // Finish remaining jobs
  cv_.notify_one();
}

const LRUBlockItem *BlockReaderCache::GetBlockReader(
    std::pair<SSTId, BlockOffset> block_info) const {
  std::shared_lock rlock(mutex_);
  auto iterator = block_reader_cache_.find(block_info);
  if (iterator == block_reader_cache_.end()) {
    return nullptr;
  }

  // Increase ref count
  iterator->second->IncRef();

  assert(iterator->second->GetRefCount() >= 2);

  return iterator->second.get();
}

const LRUBlockItem *BlockReaderCache::AddNewBlockReaderThenGet(
    std::pair<SSTId, BlockOffset> block_info,
    std::unique_ptr<LRUBlockItem> lru_block_item, bool need_to_get) const {
  // auto lru_block_item =
  //     std::make_unique<LRUBlockItem>(block_info, std::move(block_reader),
  //     this);

  // Insert new block reader into cache
  std::scoped_lock rwlock(mutex_);

  if (block_reader_cache_.size() >= capacity_) {
    Evict();
    // batch_.fetch_add(1);
    // if (batch_.load() >= 1000) {
    //   // deleted_.store(false);
    //   cv_.notify_one();
    // }
    // cv_.notify_one();
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

  if (need_to_get) {
    iterator->second->ref_count_.fetch_add(1);
  }

  assert(iterator->second->ref_count_.load() >= 1);

  return iterator->second.get();
}

// NOT THREAD-SAFE
void BlockReaderCache::Evict() const {
  if (free_list_.empty()) {
    return;
  }

  std::pair<SSTId, BlockOffset> block_info = free_list_.front();
  free_list_.pop_front();
  auto iterator = block_reader_cache_.find(block_info);

  while (iterator != block_reader_cache_.end() &&
         iterator->second->ref_count_ > 0 && !free_list_.empty()) {
    // std::cout << table_id
    //           << " table_id is in picked process to evict with ref_count =
    //           "
    //           << iterator->second->ref_count_ << std::endl;

    block_info = free_list_.front();
    iterator = block_reader_cache_.find(block_info);
    free_list_.pop_front();
  }

  // Erase from cache
  if (iterator != block_reader_cache_.end() &&
      // iterator->second->ref_count_ == 0) {
      iterator->second->ref_count_.load() <= 1) {
    // std::cout << block_info.first << " and " << block_info.second
    //           << " is evicted from cache when ref_count = "
    //           << iterator->second->ref_count_ << std::endl;
    auto deleted = block_reader_cache_.erase(block_info);
    if (deleted == 0) {
      Evict();
    }
  }
}

// NOT THREAD-SAFE
void BlockReaderCache::EvictV2() const {
  while (!shutdown_) {
    {
      std::unique_lock rwlock(mutex_);
      cv_.wait(rwlock, [this]() {
        return this->shutdown_ || !this->free_list_.empty();
      });

      // cv_.wait(rwlock, [this]() {
      //   return this->shutdown_ ||
      //          (!this->deleted_.load() && !this->free_list_.empty());
      // });

      if (this->shutdown_) {
        return;
      }

      std::pair<SSTId, BlockOffset> block_info = free_list_.front();
      while (!free_list_.empty() && !block_reader_cache_.empty() &&
             batch_.load() > 0) {
        block_info = free_list_.front();
        auto iterator = block_reader_cache_.find(block_info);
        free_list_.pop_front();

        if (iterator != block_reader_cache_.end() &&
            iterator->second->ref_count_.load() <= 1) {
          // std::cout << "namnh EVICTv2 with ref_count "
          //           << iterator->second->ref_count_ << " and block offset "
          //           << block_info.first << " and block size "
          //           << block_info.second << std::endl;

          // std::cout << "Size of block_reader_cache_ before erase "
          //           << block_reader_cache_.size() << std::endl;
          block_reader_cache_.erase(block_info);

          // std::cout << "Size of block_reader_cache_ after erase "
          //           << block_reader_cache_.size() << std::endl;
          batch_.fetch_sub(1);
        }
      }
    }

    // {
    //   std::unique_lock rwlock(mutex_);
    //   cv_.wait(rwlock, [this]() {
    //     return this->shutdown_ ||
    //            (!this->free_list_.empty() && !this->deleted_.load());
    //   });

    //   std::pair<SSTId, BlockOffset> block_info = free_list_.front();
    //   free_list_.pop_front();
    //   auto iterator = block_reader_cache_.find(block_info);

    //   while (iterator != block_reader_cache_.end() &&
    //          iterator->second->ref_count_ > 0 && !free_list_.empty()) {
    //     block_info = free_list_.front();
    //     iterator = block_reader_cache_.find(block_info);
    //     free_list_.pop_front();
    //   }

    //   // Erase from cache
    //   if (iterator != block_reader_cache_.end() &&
    //       iterator->second->ref_count_ == 0) {
    //     auto removed = block_reader_cache_.erase(block_info);
    //     if (removed == 1) {
    //       deleted_.store(true);
    //     }
    //   }
    // }
  }
}

void BlockReaderCache::AddVictim(
    std::pair<SSTId, BlockOffset> block_info) const {
  // std::cout << block_info.first << " and " << block_info.second
  //           << " victim is added" << std::endl;
  std::scoped_lock rwlock(mutex_);

  free_list_.push_back(block_info);
}

bool BlockReaderCache::CanCreateNewBlockReader() const {
  std::shared_lock rlock(mutex_);
  return block_reader_cache_.size() >= capacity_;
}

void BlockReaderCache::AddNewBlockReader(
    std::pair<SSTId, BlockOffset> block_info,
    std::unique_ptr<LRUBlockItem> lru_block_item) const {
  // Insert new block reader into cache
  std::scoped_lock rwlock(mutex_);

  if (block_reader_cache_.size() >= capacity_) {
    // std::cout << "namnh AddNewBlockReader return because size exceed"
    //           << std::endl;
    return;
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

  assert(iterator->second->ref_count_.load() >= 1);
}

db::GetStatus BlockReaderCache::GetKeyFromBlockCache(
    std::string_view key, TxnId txn_id,
    std::pair<SSTId, BlockOffset> block_info, uint64_t block_size,
    //  const TableReader *table_reader) const {
    const LRUTableItem *table_reader) const {
  assert(table_reader);
  db::GetStatus status;

  // TODO(namnh) : After getting BlockReader from BlockReaderCache, lock is
  // released. It means that we need a mechansim to ensure that TableReader is
  // not freed until operation is finished.
  // Temporarily, because of lacking cache eviction mechanism. everything is
  // fine. But when cache eviction algorithm is implemented, ref count for
  // BlockReader should be also done too
  const LRUBlockItem *block_reader = GetBlockReader(block_info);
  if (block_reader && block_reader->block_reader_) {
    // if tablereader had already been in cache
    block_reader->IncRef();
    assert(block_reader->ref_count_ >= 2);
    status = block_reader->block_reader_->SearchKey(key, txn_id, block_reader);
    thread_pool_->Enqueue(&LRUBlockItem::Unref, block_reader);
    // thread_pool_->Enqueue(&LRUTableItem::Unref, table_reader);

    return status;
  }

  // if (!CanCreateNewBlockReader()) {
  //   status.type = db::ValueType::kTooManyBlocksInMem;
  //   return status;
  // }

  // Create new tablereader
  auto new_block_reader =
      table_reader->GetTableReader()->CreateAndSetupDataForBlockReader(
          block_info.second, block_size);
  if (!new_block_reader) {
    return status;
  }

  auto new_lru_block_item = std::make_unique<LRUBlockItem>(
      block_info, std::move(new_block_reader), this);

  status = new_lru_block_item->GetBlockReader()->SearchKey(
      key, txn_id, new_lru_block_item.get());

  // if (CanCreateNewBlockReader()) {
  AddNewBlockReaderThenGet(block_info, std::move(new_lru_block_item),
                           false /*need_to_get*/);
  // AddNewBlockReader(block_info, std::move(new_lru_block_item));
  // }

  // thread_pool_->Enqueue(
  //     [this, block_info, table_reader,
  //      new_lru_block_item = std::move(new_lru_block_item)]() mutable {
  //       // if (CanCreateNewBlockReader()) {
  //       AddNewBlockReader(block_info, std::move(new_lru_block_item));
  //       // }

  //       // table_reader->Unref();
  //     });

  // thread_pool_->Enqueue(&LRUTableItem::Unref, table_reader);

  return status;
}

} // namespace sstable

} // namespace kvs