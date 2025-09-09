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
#include <numeric>
#include <ranges>

namespace kvs {

namespace db {

DBImpl::DBImpl(bool is_testing)
    : next_sstable_id_(0), memtable_(std::make_unique<MemTable>()),
      txn_manager_(std::make_unique<mvcc::TransactionManager>(this)),
      config_(std::make_unique<Config>(is_testing)),
      thread_pool_(new kvs::ThreadPool()),
      version_manager_(std::make_unique<VersionManager>(this, config_.get(),
                                                        thread_pool_)) {}

DBImpl::~DBImpl() {
  delete thread_pool_;
  thread_pool_ = nullptr;
}

void DBImpl::LoadDB() {
  config_->LoadConfig();

  Version *latest_version = version_manager_->CreateLatestVersion();
  if (!latest_version) {
    // return;
    std::exit(EXIT_FAILURE);
  }

  // TODO(namnh) : Read ALL of SST files and init there info
}

std::optional<std::string> DBImpl::Get(std::string_view key, TxnId txn_id) {
  GetStatus status;
  {
    std::shared_lock rlock(mutex_);

    // Find data from Memtable
    status = memtable_->Get(key, txn_id);
    if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
      return status.value;
    }

    // If key is not found, continue finding from immutable memtables
    for (const auto &immu_memtable :
         immutable_memtables_ | std::views::reverse) {
      status = immu_memtable->Get(key, txn_id);
      if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
        return status.value;
      }
    }
  }

  // TODO(nanmh) : Does it need to acquire lock when looking up key in SSTs?
  const Version *version = version_manager_->GetLatestVersion();
  if (!version) {
    return std::nullopt;
  }

  status = version->Get(key, txn_id);
  if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
    return status.value;
  }

  return std::nullopt;
}

void DBImpl::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  std::unique_lock rwlock(mutex_);
  // TODO(namnh): Write Stop problem. Improve in future
  cv_.wait(rwlock, [this]() {
    return immutable_memtables_.size() < config_->GetMaxImmuMemTablesInMem();
  });

  memtable_->Put(key, value, txn_id);

  if (memtable_->GetMemTableSize() >= config_->GetPerMemTableSizeLimit()) {
    immutable_memtables_.push_back(std::move(memtable_));

    if (immutable_memtables_.size() >= config_->GetMaxImmuMemTablesInMem()) {
      // Flush thread to flush memtable to disk
      thread_pool_->Enqueue(&DBImpl::FlushMemTableJob, this);
    }

    // Create new empty mutable memtable
    memtable_ = std::make_unique<MemTable>();
  }
}

void DBImpl::ForceFlushMemTable() {
  {
    std::scoped_lock lock(mutex_);
    if (memtable_->GetMemTableSize() == 0) {
      return;
    }

    immutable_memtables_.push_back(std::move(memtable_));
    // Create new empty mutable memtable
    memtable_ = std::make_unique<MemTable>();
  }

  return FlushMemTableJob();
}

void DBImpl::FlushMemTableJob() {
  // Key point: Db DOES NOT need to acquire mutex here. Because
  // CreateLatestVersion is protected by mutex. So, at a time, there is always 1
  // thread/process can access. It means that, each latest version returned is
  // ensured to be race condition free
  Version *latest_version = version_manager_->CreateLatestVersion();
  if (!latest_version) {
    return;
  }

  latest_version->CreateNewSSTs(immutable_memtables_);

  // TODO(namnh) : Update manifest info
  // After sst is persisted to disk and manifest is updated, remove immutable
  // memtable from memory
  {
    std::scoped_lock rwlock(mutex_);
    immutable_memtables_.clear();
    cv_.notify_all();
  }

  if (version_manager_->NeedSSTCompaction()) {
    TriggerCompaction();
  }
}

void DBImpl::TriggerCompaction() {
  // Key point: Db DOES NOT need to acquire mutex here. Because
  // CreateLatestVersion is protected by mutex. So, at a time, there is always 1
  // thread/process can access. It means that, each latest version returned is
  // ensured to be race condition free
  Version *latest_version = version_manager_->CreateLatestVersion();
  if (!latest_version) {
    return;
  }

  thread_pool_->Enqueue(&Version::ExecCompaction, latest_version);
}

uint64_t DBImpl::GetNextSSTId() {
  next_sstable_id_.fetch_add(1);
  return next_sstable_id_.load();
}

const Config *const DBImpl::GetConfig() { return config_.get(); }

const VersionManager *DBImpl::GetVersionManager() {
  return version_manager_.get();
}

// SST INFO
DBImpl::SSTInfo::SSTInfo(std::unique_ptr<sstable::Table> table)
    : table_(std::move(table)) {}

} // namespace db

} // namespace kvs