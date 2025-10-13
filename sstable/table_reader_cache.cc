#include "sstable/table_reader_cache.h"

#include "common/thread_pool.h"
#include "db/db_impl.h"
#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_reader.h"

namespace kvs {

namespace sstable {

TableReaderCache::TableReaderCache(const db::DBImpl *db,
                                   kvs::ThreadPool *thread_pool)
    : capacity_(1000), shutdown_(false), db_(db), thread_pool_(thread_pool) {
  assert(db_ && thread_pool_);
  // thread_pool_->Enqueue(&TableReaderCache::EvictV2, this);
  // thread_pool_->Enqueue(&TableReaderCache::PeriodicCleanupCache, this);
}

TableReaderCache::~TableReaderCache() {
  shutdown_.store(true);

  cv_.notify_one();
}

const LRUTableItem *TableReaderCache::GetLRUTableItem(SSTId table_id) const {
  std::shared_lock rlock(mutex_);
  auto iterator = table_readers_cache_.find(table_id);
  if (iterator == table_readers_cache_.end()) {
    return nullptr;
  }

  // Increase ref count
  iterator->second->IncRef();

  assert(iterator->second->GetRefCount() >= 2);

  return iterator->second.get();
}

const LRUTableItem *TableReaderCache::AddNewTableReaderThenGet(
    SSTId table_id, std::unique_ptr<LRUTableItem> lru_table_item) const {
  std::scoped_lock rwlock(mutex_);
  if (table_readers_cache_.size() >= capacity_) {
    Evict();
    // cv_.notify_one();
  }

  bool success =
      table_readers_cache_.insert({table_id, std::move(lru_table_item)}).second;

  // Get block reader that MAYBE inserted
  auto iterator = table_readers_cache_.find(table_id);
  if (success) {
    // Increase ref count
    iterator->second->IncRef();
  }

  // Increase ref count
  iterator->second->IncRef();

  return iterator->second.get();
}

// NOT THREAD-SAFE
void TableReaderCache::Evict() const {
  if (free_list_.empty()) {
    return;
  }

  SSTId table_id = free_list_.front();
  while (!free_list_.empty() && !table_readers_cache_.empty()) {
    // while (table_readers_cache_.size() >= capacity_) {
    table_id = free_list_.front();
    auto iterator = table_readers_cache_.find(table_id);
    free_list_.pop_front();

    if (iterator != table_readers_cache_.end() &&
        iterator->second->ref_count_.load() <= 1) {
      std::cout << "namnh Try to evict table file descriptor" << std::endl;
      table_readers_cache_.erase(table_id);
    }
  }
}

// NOT THREAD-SAFE
void TableReaderCache::EvictV2() const {
  while (!shutdown_) {
    {
      std::unique_lock rwlock(mutex_);
      cv_.wait(rwlock, [this]() {
        return this->shutdown_ || !this->free_list_.empty();
      });

      if (this->shutdown_) {
        std::cout << "namnh MUST SHUTDOWN BlockReaderCache::EvictV2"
                  << std::endl;
        return;
      }

      SSTId table_id = free_list_.front();
      while (!free_list_.empty() && !table_readers_cache_.empty()) {
        // while (table_readers_cache_.size() >= capacity_) {
        table_id = free_list_.front();
        auto iterator = table_readers_cache_.find(table_id);
        free_list_.pop_front();

        if (iterator != table_readers_cache_.end() &&
            iterator->second->ref_count_.load() <= 1) {
          table_readers_cache_.erase(table_id);
        }
      }
    }
  }
}

void TableReaderCache::PeriodicCleanupCache() const {
  while (!shutdown_) {
    {
      std::scoped_lock rwlock(mutex_);
      cv_.notify_one();
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

void TableReaderCache::AddVictim(SSTId table_id) const {
  std::scoped_lock rwlock(mutex_);
  free_list_.push_back(table_id);
}

db::GetStatus TableReaderCache::GetKeyFromTableCache(
    std::string_view key, TxnId txn_id, SSTId table_id, uint64_t file_size,
    const sstable::BlockReaderCache *block_reader_cache) const {
  db::GetStatus status;

  // TODO(namnh) : After getting TableReader from TableReaderCache, lock is
  // released. It means that we need a mechansim to ensure that TableReader is
  // not freed until operation is finished.
  // Temporarily, because of lacking cache eviction mechanism. everything is
  // fine. But when cache eviction algorithm is implemented, ref count for
  // TableReader should be also done too
  // const TableReader *table_reader = GetTableReader(table_id);
  const LRUTableItem *table_reader = GetLRUTableItem(table_id);
  if (table_reader && table_reader->table_reader_) {
    // if table reader had already been in cache
    status = table_reader->table_reader_->SearchKey(
        key, txn_id, block_reader_cache, table_reader);
    thread_pool_->Enqueue(&LRUTableItem::Unref, table_reader);
    return status;
  }

  // if table hadn't been in cache, create new table and load into cache
  std::string filename = db_->GetDBPath() + std::to_string(table_id) + ".sst";

  // Create new table reader
  auto new_table_reader = CreateAndSetupDataForTableReader(std::move(filename),
                                                           table_id, file_size);
  if (!new_table_reader) {
    // throw std::runtime_error("Can't open SST file to read");
    status.type = db::ValueType::kTooManyOpenFiles;

    filename = db_->GetDBPath() + std::to_string(table_id) + ".sst";
    db_->WakeupBgThreadToCleanupFiles(filename);
    return status;
  }

  auto new_lru_table_item = std::make_unique<LRUTableItem>(
      table_id, std::move(new_table_reader), this);
  status = new_lru_table_item->GetTableReader()->SearchKey(
      key, txn_id, block_reader_cache, new_lru_table_item.get());

  thread_pool_->Enqueue(
      [this, table_id,
       new_lru_table_item = std::move(new_lru_table_item)]() mutable {
        const LRUTableItem *new_table_added =
            AddNewTableReaderThenGet(table_id, std::move(new_lru_table_item));
        new_table_added->Unref();
      });

  return status;
}

} // namespace sstable

} // namespace kvs