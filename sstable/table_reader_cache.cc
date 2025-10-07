#include "sstable/table_reader_cache.h"

#include "db/db_impl.h"
#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_reader.h"

namespace kvs {

namespace sstable {

TableReaderCache::TableReaderCache(const db::DBImpl *db)
    : capacity_(10), db_(db) {
  assert(db_);
}

const LRUTableItem *TableReaderCache::GetTableReader(SSTId table_id) const {
  std::shared_lock rlock(mutex_);
  auto iterator = table_readers_cache_.find(table_id);
  if (iterator == table_readers_cache_.end()) {
    return nullptr;
  }

  // Increase ref count
  iterator->second->ref_count_.fetch_add(1);

  return iterator->second.get();
}

const LRUTableItem *TableReaderCache::AddNewTableReaderThenGet(
    SSTId table_id, std::unique_ptr<TableReader> table_reader) const {
  auto lru_table_item =
      std::make_unique<LRUTableItem>(table_id, std::move(table_reader), this);

  std::scoped_lock rwlock(mutex_);
  if (table_readers_cache_.size() >= capacity_) {
    Evict();
  }

  table_readers_cache_.insert({table_id, std::move(lru_table_item)});

  // Get block reader that MAYBE inserted
  auto iterator = table_readers_cache_.find(table_id);
  // Increase ref count
  iterator->second->ref_count_.fetch_add(1);

  return iterator->second.get();
}

// NOT THREAD-SAFE
void TableReaderCache::Evict() const {
  if (free_list_.empty()) {
    return;
  }

  SSTId table_id = free_list_.front();
  free_list_.pop_front();
  auto iterator = table_readers_cache_.find(table_id);

  // if (iterator != table_readers_cache_.end()) {
  //   std::cout << "NAMNH CHECK ref count of FIRST VICTIM "
  //             << iterator->second->ref_count_ << std::endl;
  // }

  while (iterator != table_readers_cache_.end() &&
         iterator->second->ref_count_ > 0 && !free_list_.empty()) {
    std::cout << table_id
              << " table_id is in picked process to evict with ref_count = "
              << iterator->second->ref_count_ << std::endl;

    table_id = free_list_.front();
    iterator = table_readers_cache_.find(table_id);
    free_list_.pop_front();
  }

  // Erase from cache
  if (iterator != table_readers_cache_.end() &&
      iterator->second->ref_count_ == 0) {
    // std::cout << table_id << " is evicted from cache when ref_count = "
    //           << iterator->second->ref_count_ << std::endl;
    table_readers_cache_.erase(table_id);
  }
}

void TableReaderCache::AddVictim(SSTId table_id) const {
  // std::cout << table_id << " victim is added" << std::endl;

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
  const LRUTableItem *table_reader = GetTableReader(table_id);
  if (table_reader && table_reader->table_reader_) {
    // if table reader had already been in cache
    status = table_reader->table_reader_->SearchKey(
        key, txn_id, block_reader_cache, table_reader);
    return status;
  }

  // if table hadn't been in cache, create new table and load into cache
  std::string filename = db_->GetDBPath() + std::to_string(table_id) + ".sst";

  // Create new table reader
  auto new_table_reader = CreateAndSetupDataForTableReader(std::move(filename),
                                                           table_id, file_size);
  if (!new_table_reader) {
    throw std::runtime_error("Can't open SST file to read");
  }

  // Search key in new table
  // status = lru_table_item->GetTableReader()->SearchKey(
  //     key, txn_id, block_reader_cache, lru_table_item.get());

  // {
  //   // Insert table into cache
  //   std::scoped_lock rwlock(mutex_);
  //   table_readers_cache_.insert({table_id, std::move(lru_table_item)});
  // }

  const LRUTableItem *lru_table_item =
      AddNewTableReaderThenGet(table_id, std::move(new_table_reader));
  assert(lru_table_item);

  status = lru_table_item->GetTableReader()->SearchKey(
      key, txn_id, block_reader_cache, lru_table_item);

  return status;
}

} // namespace sstable

} // namespace kvs