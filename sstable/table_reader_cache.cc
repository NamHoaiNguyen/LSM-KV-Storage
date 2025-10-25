#include "sstable/table_reader_cache.h"

#include "common/thread_pool.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_reader.h"

namespace kvs {

namespace sstable {

TableReaderCache::TableReaderCache(const db::DBImpl *db,
                                   const kvs::ThreadPool *const thread_pool)
    : shutdown_(false), capacity_(db->GetConfig()->GetTotalTablesCache()),
      db_(db), thread_pool_(thread_pool) {
  assert(db_ && thread_pool_);
  thread_pool_->Enqueue(&TableReaderCache::ExecuteBgThread, this);
}

TableReaderCache::~TableReaderCache() {
  shutdown_.store(true);

  bg_cv_.notify_one();
}

// const LRUTableItem *TableReaderCache::GetLRUTableItem(SSTId table_id) const {
std::shared_ptr<LRUTableItem>
TableReaderCache::GetLRUTableItem(SSTId table_id) const {
  std::shared_lock rlock(mutex_);
  auto iterator = table_readers_cache_.find(table_id);
  if (iterator == table_readers_cache_.end()) {
    return nullptr;
  }

  // Increase ref count
  iterator->second->IncRef();

  return iterator->second;
}

std::shared_ptr<LRUTableItem> TableReaderCache::AddNewTableReaderThenGet(
    SSTId table_id, std::shared_ptr<LRUTableItem> lru_table_item,
    bool add_then_get) const {
  std::scoped_lock rwlock(mutex_);
  if (table_readers_cache_.size() >= capacity_) {
    Evict();
  }

  bool success =
      table_readers_cache_.insert({table_id, std::move(lru_table_item)}).second;

  // Get block reader that MAYBE inserted
  auto iterator = table_readers_cache_.find(table_id);

  if (add_then_get) {
    // Increase ref count if need to get
    iterator->second->IncRef();
  }

  if (iterator->second->GetRefCount() < 1) {
    // This TableReader should be put in free_list_
    free_list_.push_back(table_id);
  }

  return iterator->second;
}

// NOT THREAD-SAFE
void TableReaderCache::Evict() const {
  if (free_list_.empty() || table_readers_cache_.empty()) {
    return;
  }

  SSTId table_id = free_list_.front();
  while (!free_list_.empty() && !table_readers_cache_.empty()) {
    table_id = free_list_.front();
    auto iterator = table_readers_cache_.find(table_id);
    free_list_.pop_front();

    if (iterator != table_readers_cache_.end() &&
        iterator->second->ref_count_.load() < 1) {
      table_readers_cache_.erase(table_id);
    }
  }
}

void TableReaderCache::AddVictim(SSTId table_id) const {
  std::scoped_lock rwlock(mutex_);
  free_list_.push_back(table_id);
}

void TableReaderCache::ExecuteBgThread() const {
  while (!shutdown_) {
    {
      std::unique_lock rwlock_bg(bg_mutex_);
      bg_cv_.wait(rwlock_bg, [this]() {
        return this->shutdown_ || !this->item_cache_queue_.empty() ||
               !this->victim_queue_.empty();
      });

      if (this->shutdown_ && this->item_cache_queue_.empty() &&
          this->victim_queue_.empty()) {
        return;
      }

      while (!victim_queue_.empty()) {
        std::shared_ptr<LRUTableItem> item = victim_queue_.front();
        victim_queue_.pop();

        item->Unref();
      }

      while (!item_cache_queue_.empty()) {
        ItemCache item = item_cache_queue_.front();
        item_cache_queue_.pop();

        AddNewTableReaderThenGet(item.table_id, item.item, item.add_then_get);
      }
    }
  }
}

db::GetStatus TableReaderCache::GetValue(
    std::string_view key, TxnId txn_id, SSTId table_id, uint64_t file_size,
    const sstable::BlockReaderCache *const block_reader_cache) const {
  db::GetStatus status;

  std::shared_ptr<LRUTableItem> lru_table_item = GetLRUTableItem(table_id);
  if (lru_table_item && lru_table_item->GetTableReader()) {
    // if table reader had already been in cache
    status = lru_table_item->table_reader_->GetValue(
        key, txn_id, block_reader_cache, lru_table_item->GetTableReader());
    {
      std::scoped_lock rwlock_bg(bg_mutex_);
      victim_queue_.push(lru_table_item);
    }
    bg_cv_.notify_one();

    return status;
  }

  // if table hadn't been in cache, create new table and load into cache
  std::string filename = db_->GetDBPath() + std::to_string(table_id) + ".sst";

  // Create new table reader
  auto new_table_reader = CreateAndSetupDataForTableReader(std::move(filename),
                                                           table_id, file_size);
  if (!new_table_reader) {
    status.type = db::ValueType::kTooManyOpenFiles;
    return status;
  }

  auto new_lru_table_item = std::make_shared<LRUTableItem>(
      table_id, std::move(new_table_reader), this);
  status = new_lru_table_item->GetTableReader()->GetValue(
      key, txn_id, block_reader_cache, new_lru_table_item->GetTableReader());

  {
    std::scoped_lock rwlock_bg(bg_mutex_);
    item_cache_queue_.push(
        {table_id, new_lru_table_item, false /*need_to_get*/});
  }
  bg_cv_.notify_one();

  return status;
}

} // namespace sstable

} // namespace kvs