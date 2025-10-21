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
                                   kvs::ThreadPool *thread_pool)
    : capacity_(db->GetConfig()->GetTotalTablesCache()), db_(db),
      thread_pool_(thread_pool) {
  assert(db_ && thread_pool_);
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

// const LRUTableItem *TableReaderCache::AddNewTableReaderThenGet(
std::shared_ptr<LRUTableItem> TableReaderCache::AddNewTableReaderThenGet(
    SSTId table_id, std::unique_ptr<LRUTableItem> lru_table_item,
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
  if (free_list_.empty()) {
    return;
  }

  if (table_readers_cache_.empty()) {
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

db::GetStatus TableReaderCache::GetKey(
    std::string_view key, TxnId txn_id, SSTId table_id, uint64_t file_size,
    const sstable::BlockReaderCache *block_reader_cache) const {
  db::GetStatus status;

  std::shared_ptr<LRUTableItem> table_reader = GetLRUTableItem(table_id);
  if (table_reader && table_reader->GetTableReader()) {
    // if table reader had already been in cache
    status = table_reader->table_reader_->GetValue(
        key, txn_id, block_reader_cache, table_reader->GetTableReader());
    thread_pool_->Enqueue(&LRUTableItem::Unref, table_reader);
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

  auto new_lru_table_item = std::make_unique<LRUTableItem>(
      table_id, std::move(new_table_reader), this);
  status = new_lru_table_item->GetTableReader()->GetValue(
      key, txn_id, block_reader_cache, new_lru_table_item->GetTableReader());

  thread_pool_->Enqueue(
      [this, table_id,
       new_lru_table_item = std::move(new_lru_table_item)]() mutable {
        AddNewTableReaderThenGet(table_id, std::move(new_lru_table_item),
                                 false /*add_then_get*/);
      });

  return status;
}

} // namespace sstable

} // namespace kvs