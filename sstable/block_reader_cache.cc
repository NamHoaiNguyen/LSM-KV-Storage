#include "sstable/block_reader_cache.h"

#include "sstable/block_reader.h"
#include "sstable/lru_block_item.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"

#include <iostream>

namespace kvs {

namespace sstable {

BlockReaderCache::BlockReaderCache() : capacity_(100000) {}

const LRUBlockItem *BlockReaderCache::GetBlockReader(
    std::pair<SSTId, BlockOffset> block_info) const {
  std::shared_lock rlock(mutex_);
  auto iterator = block_reader_cache_.find(block_info);
  if (iterator == block_reader_cache_.end()) {
    return nullptr;
  }

  // Increase ref count
  iterator->second->ref_count_.fetch_add(1);

  return iterator->second.get();
}

const LRUBlockItem *BlockReaderCache::AddNewBlockReaderThenGet(
    std::pair<SSTId, BlockOffset> block_info,
    std::unique_ptr<BlockReader> block_reader) const {
  auto lru_block_item =
      std::make_unique<LRUBlockItem>(block_info, std::move(block_reader), this);

  // Insert new block reader into cache
  std::scoped_lock rwlock(mutex_);

  if (block_reader_cache_.size() >= capacity_) {
    Evict();
  }

  block_reader_cache_.insert({block_info, std::move(lru_block_item)});

  // Get block reader that MAYBE inserted
  auto iterator = block_reader_cache_.find(block_info);

  // Increase ref count
  iterator->second->ref_count_.fetch_add(1);

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

  // if (iterator != table_readers_cache_.end()) {
  //   std::cout << "NAMNH CHECK ref count of FIRST VICTIM "
  //             << iterator->second->ref_count_ << std::endl;
  // }

  while (iterator != block_reader_cache_.end() &&
         iterator->second->ref_count_ > 0 && !free_list_.empty()) {
    // std::cout << table_id
    //           << " table_id is in picked process to evict with ref_count = "
    //           << iterator->second->ref_count_ << std::endl;

    block_info = free_list_.front();
    iterator = block_reader_cache_.find(block_info);
    free_list_.pop_front();
  }

  // Erase from cache
  if (iterator != block_reader_cache_.end() &&
      iterator->second->ref_count_ == 0) {
    std::cout << block_info.first << " and " << block_info.second
              << " is evicted from cache when ref_count = "
              << iterator->second->ref_count_ << std::endl;
    block_reader_cache_.erase(block_info);
  }
}

void BlockReaderCache::AddVictim(
    std::pair<SSTId, BlockOffset> block_info) const {
  std::cout << block_info.first << " and " << block_info.second
            << " victim is added" << std::endl;
  std::scoped_lock rwlock(mutex_);

  free_list_.push_back(block_info);
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
    status = block_reader->block_reader_->SearchKey(key, txn_id);
    return status;
  }

  // Create new tablereader
  auto new_block_reader =
      table_reader->GetTableReader()->CreateAndSetupDataForBlockReader(
          block_info.second, block_size);

  status = new_block_reader->SearchKey(key, txn_id);

  // {
  //   // Insert new blockreader into cache
  //   std::scoped_lock rwlock(mutex_);
  //   block_reader_cache_.insert({block_info, std::move(new_block_reader)});
  // }

  const LRUBlockItem *lru_block_item =
      AddNewBlockReaderThenGet(block_info, std::move(new_block_reader));
  assert(lru_block_item);

  status = lru_block_item->GetBlockReader()->SearchKey(key, txn_id);

  return status;
}

} // namespace sstable

} // namespace kvs