#include "db/db_impl.h"

#include "common/base_iterator.h"
#include "common/thread_pool.h"
#include "db/compact.h"
#include "db/config.h"
#include "db/memtable.h"
#include "db/memtable_iterator.h"
#include "db/skiplist.h"
#include "db/skiplist_iterator.h"
#include "db/status.h"
#include "db/version.h"
#include "db/version_edit.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "mvcc/transaction.h"
#include "mvcc/transaction_manager.h"
#include "sstable/block_builder.h"
#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"

// libC++
#include <algorithm>
#include <cassert>
#include <cmath>
#include <numeric>
#include <ranges>

namespace kvs {

namespace db {

DBImpl::DBImpl(bool is_testing)
    : next_sstable_id_(0), memtable_(std::make_unique<MemTable>()),
      txn_manager_(std::make_unique<mvcc::TransactionManager>(this)),
      config_(std::make_unique<Config>(is_testing)),
      background_compaction_scheduled_(false),
      thread_pool_(new kvs::ThreadPool()),
      table_reader_cache_(
          std::make_unique<sstable::TableReaderCache>(config_.get())),
      block_reader_cache_(std::make_unique<sstable::BlockReaderCache>()),
      version_manager_(std::make_unique<VersionManager>(
          this, table_reader_cache_.get(), block_reader_cache_.get(),
          config_.get(), thread_pool_)) {}

DBImpl::~DBImpl() {
  delete thread_pool_;
  thread_pool_ = nullptr;
}

void DBImpl::LoadDB() {
  config_->LoadConfig();
  // TODO(namnh) : remove after finish recovering flow
  compact_pointer_.resize(config_->GetSSTNumLvels());
  version_manager_->CreateLatestVersion();
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

  version->IncreaseRefCount();
  status = version->Get(key, txn_id);
  if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
    return status.value;
  }
  version->DecreaseRefCount();

  return std::nullopt;
}

void DBImpl::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  std::unique_lock rwlock(mutex_);
  // Stop writing when numbers of immutable memtable reach to threshold
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

// Just for testing
void DBImpl::ForceFlushMemTable() {
  {
    std::scoped_lock lock(mutex_);
    if (memtable_->GetMemTableSize() != 0) {
      immutable_memtables_.push_back(std::move(memtable_));
      // Create new empty mutable memtable
      memtable_ = std::make_unique<MemTable>();
    }
  }

  FlushMemTableJob();
}

void DBImpl::FlushMemTableJob() {
  // Create new SSTs
  std::latch all_done(immutable_memtables_.size());

  std::vector<std::shared_ptr<SSTMetadata>> new_ssts_info;
  auto version_edit = std::make_unique<VersionEdit>();

  {
    std::scoped_lock rwlock(mutex_);
    for (const auto &immutable_memtable : immutable_memtables_) {
      thread_pool_->Enqueue(&DBImpl::CreateNewSST, this,
                            std::cref(immutable_memtable), version_edit.get(),
                            std::ref(all_done));
    }
  }

  // Wait until all workers have finished
  all_done.wait();

  // Not until this point that latest version is visible
  version_manager_->ApplyNewChanges(std::move(version_edit));

  // Notify to let writing continues.
  // NOTE: new writes are only allowed after new version is VISIBLE
  {
    std::scoped_lock rwlock(mutex_);
    immutable_memtables_.clear();
    cv_.notify_all();
  }

  MaybeScheduleCompaction();
}

void DBImpl::CreateNewSST(
    const std::unique_ptr<BaseMemTable> &immutable_memtable,
    VersionEdit *version_edit, std::latch &work_done) {
  assert(version_edit);

  uint64_t sst_id = GetNextSSTId();

  std::string filename =
      config_->GetSavedDataPath() + std::to_string(sst_id) + ".sst";
  sstable::TableBuilder new_sst(std::move(filename), config_.get());

  if (!new_sst.Open()) {
    work_done.count_down();
    return;
  }

  auto iterator = std::make_unique<MemTableIterator>(immutable_memtable.get());
  // Iterate through all key/value pairs to add them to sst
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    new_sst.AddEntry(iterator->GetKey(), iterator->GetValue(),
                     iterator->GetTransactionId(), iterator->GetType());
  }

  // All of key/value pairs had been written to sst. SST should be flushed to
  // disk
  new_sst.Finish();

  {
    std::scoped_lock lock(mutex_);
    std::string filename =
        config_->GetSavedDataPath() + std::to_string(sst_id) + ".sst";
    version_edit->AddNewFiles(sst_id, 0 /*level*/, new_sst.GetFileSize(),
                              new_sst.GetSmallestKey(), new_sst.GetLargestKey(),
                              std::move(filename));
  }

  // Signal that this worker is done
  work_done.count_down();
}

void DBImpl::MaybeScheduleCompaction() {
  if (background_compaction_scheduled_) {
    // only 1 compaction happens at a moment. This condition is highest
    // privilege
    return;
  }

  if (!version_manager_->NeedSSTCompaction()) {
    return;
  }

  background_compaction_scheduled_ = true;
  thread_pool_->Enqueue(&DBImpl::ExecuteBackgroundCompaction, this);
}

void DBImpl::ExecuteBackgroundCompaction() {
  const Version *version = version_manager_->GetLatestVersion();
  if (!version) {
    return;
  }

  version->IncreaseRefCount();
  auto version_edit = std::make_unique<VersionEdit>();
  auto compact = std::make_unique<Compact>(block_reader_cache_.get(),
                                           table_reader_cache_.get(), version,
                                           version_edit.get());
  compact->PickCompact();
  version->DecreaseRefCount();

  // Apply compact version edit(changes) to create new version
  version_manager_->ApplyNewChanges(std::move(version_edit));

  // Compaction can create many files, so maybe we need another compaction round
  MaybeScheduleCompaction();
}

uint64_t DBImpl::GetNextSSTId() {
  next_sstable_id_.fetch_add(1);
  return next_sstable_id_.load();
}

const Config *const DBImpl::GetConfig() { return config_.get(); }

const VersionManager *DBImpl::GetVersionManager() const {
  return version_manager_.get();
}

const BaseMemTable *DBImpl::GetCurrentMemtable() { return memtable_.get(); }

const std::vector<std::unique_ptr<BaseMemTable>> &
DBImpl::GetImmutableMemTables() {
  return immutable_memtables_;
}

const sstable::BlockReaderCache *DBImpl::GetBlockReaderCache() const {
  return block_reader_cache_.get();
}

} // namespace db

} // namespace kvs