#include "db/db_impl.h"

#include "common/base_iterator.h"
#include "common/thread_pool.h"
#include "db/compact.h"
#include "db/config.h"
#include "db/memtable.h"
#include "db/memtable_iterator.h"
#include "db/status.h"
#include "db/version.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "mvcc/transaction.h"
#include "mvcc/transaction_manager.h"
#include "sstable/block.h"
#include "sstable/block_index.h"
#include "sstable/sst.h"

// libC++
#include <algorithm>
#include <cmath>
#include <ranges>

namespace kvs {

namespace db {

DBImpl::DBImpl(std::string_view dbname)
    : dbname_(std::string(dbname)), next_sstable_id_(0),
      memtable_(std::make_unique<MemTable>()),
      txn_manager_(std::make_unique<mvcc::TransactionManager>(this)),
      config_(std::make_unique<Config>()),
      version_manager_(std::make_unique<VersionManager>(this, config_.get())),
      thread_pool_(new kvs::ThreadPool()) {}

DBImpl::~DBImpl() {
  delete thread_pool_;
  thread_pool_ = nullptr;
}

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

  if (memtable_->GetMemTableSize() >= config_->GetPerMemTableSizeLimit()) {
    // memtable_->SetImmutable();
    immutable_memtables_.push_back(std::move(memtable_));

    if (immutable_memtables_.size() >= config_->GetMaxImmuMemTablesInMem()) {
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
  Version *latest_version = version_manager_->CreateNewVersion();
  if (!latest_version) {
    return;
  }

  if (!latest_version->CreateNewSST(immutable_memtable)) {
    return;
  }

  // After sst is persisted to disk, remove immutable memtable from memory
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

uint64_t DBImpl::GetNextSSTId() {
  next_sstable_id_.fetch_add(1);
  return next_sstable_id_.load();
}

// SST INFO
DBImpl::SSTInfo::SSTInfo(std::unique_ptr<sstable::Table> table)
    : table_(std::move(table)) {}

} // namespace db

} // namespace kvs