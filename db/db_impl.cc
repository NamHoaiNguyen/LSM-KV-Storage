#include "db/db_impl.h"

#include "common/base_iterator.h"
#include "common/thread_pool.h"
#include "db/compact.h"
#include "db/config.h"
#include "db/memtable.h"
#include "db/memtable_iterator.h"
#include "db/status.h"
#include "io/base_file.h"
#include "mvcc/transaction.h"
#include "mvcc/transaction_manager.h"
#include "sstable/block.h"
#include "sstable/block_index.h"
#include "sstable/sst.h"

// libC++
#include <cmath>
#include <ranges>

namespace {

// TODO(namnh) : dynamic
constexpr size_t kMemTableSize = 4 * 1024 * 1024; // MB
constexpr size_t kDebugMemTableSize = 32;         // Bytes

// TODO(namnh) : dynamic(Yeah, I want to challenge myself!!!)
constexpr size_t kMaxNumberImmutableTablesInMemory = 3;
constexpr size_t kDebugMaxNumberImmutableTablesInMemory = 1;

namespace {
const std::string path = "/home/hoainam/self/biggg/lsm-kv-storage/data/";

// TODO(namnh) : Dynamic!
constexpr int kL0_CompactionTrigger = 1;

uint64_t MaxSizePerLevel(uint8_t level) {
  return static_cast<uint64_t>(std::pow((10 * 1024 * 1024), level));
}

} // namespace

} // namespace

namespace kvs {

namespace db {

DBImpl::DBImpl(std::string_view dbname)
    : dbname_(std::string(dbname)), next_sstable_id_(0),
      memtable_(std::make_unique<MemTable>()),
      compact_(std::make_unique<Compact>(this)),
      txn_manager_(std::make_unique<mvcc::TransactionManager>(this)),
      current_L0_files_(0), thread_pool_(new ThreadPool()) {}

DBImpl::~DBImpl() { delete thread_pool_; }

std::optional<std::string> DBImpl::Get(std::string_view key, TxnId txn_id) {
  std::shared_lock rlock(mutex_);
  GetStatus status;

  // Find data from Memtable
  status = memtable_->Get(key, txn_id);
  if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
    return status.value;
  }

  // If key is not found, continue finding from immutable memtables

  for (const auto &immu_memtable : immutable_memtables_ | std::views::reverse) {
    status = immu_memtable->Get(key, txn_id);
    if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
      return status.value;
    }
  }

  // TODO(namnh) : Find from sstable
  return std::nullopt;
}

void DBImpl::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  std::scoped_lock rwlock(mutex_);
  memtable_->Put(key, value, txn_id);

  if (memtable_->GetMemTableSize() >= kDebugMemTableSize) {
    // memtable_->SetImmutable();
    immutable_memtables_.push_back(std::move(memtable_));

    if (immutable_memtables_.size() >= kDebugMaxNumberImmutableTablesInMemory) {
      // Flush thread to flush memtable to disk
      for (const auto &immu_memtable : immutable_memtables_) {
        thread_pool_->Enqueue(&DBImpl::FlushMemTableJob, this,
                              immu_memtable.get());
      }
    }

    // Create new empty mutable memtable
    memtable_ = std::make_unique<MemTable>();
  }
}

void DBImpl::FlushMemTableJob(const BaseMemTable *const immutable_memtable) {
  CreateNewSST(immutable_memtable);

  // Execute compaction if need
  if (current_L0_files_ >= kL0_CompactionTrigger) {
    thread_pool_->Enqueue(&Compact::PickCompact, compact_.get(),
                          levels_sst_info_[0].size());
  }
}

void DBImpl::CreateNewSST(const BaseMemTable *const immutable_memtable) {
  std::string next_sst = std::to_string(next_sstable_id_.fetch_add(1));
  std::string filename = path + next_sst + ".sst";
  auto new_sst = std::make_unique<sstable::Table>(std::move(filename));
  if (!new_sst->Open()) {
    throw std::runtime_error("Can't create sstable file");
  }

  std::unique_ptr<MemTableIterator> iterator;
  {
    // TODO(namnh) : Do we need to acquire lock ?
    // std::scoped_lock rwlock(mutex_);
    iterator = std::make_unique<MemTableIterator>(immutable_memtable);
  }
  // Iterate through all key/value pairs to add them to sst
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    new_sst->AddEntry(iterator->GetKey(), iterator->GetValue(),
                      iterator->GetTransactionId(), iterator->GetType());
  }

  // All of key/value pairs had been written to sst. SST should be flushed to
  // disk
  new_sst->Finish();

  current_L0_files_++;

  // Save info of new sst
  levels_sst_info_[0].emplace_back(
      std::make_unique<SSTInfo>(std::move(new_sst)));

  {
    std::scoped_lock rwlock(mutex_);
    immutable_memtables_.erase(
        std::remove_if(immutable_memtables_.begin(), immutable_memtables_.end(),
                       [immutable_memtable](const auto &ptr) {
                         return ptr.get() == immutable_memtable;
                       }),
        immutable_memtables_.end());
  }
}

// SST INFO
DBImpl::SSTInfo::SSTInfo(std::unique_ptr<sstable::Table> table)
    : table_(std::move(table)) {}

} // namespace db

} // namespace kvs