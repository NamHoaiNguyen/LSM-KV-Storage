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

DBImpl::DBImpl()
    : next_sstable_id_(0), memtable_(std::make_unique<MemTable>()),
      txn_manager_(std::make_unique<mvcc::TransactionManager>(this)),
      config_(std::make_unique<Config>()), thread_pool_(new kvs::ThreadPool()),
      version_manager_(
          std::make_unique<VersionManager>(this, config_.get(), thread_pool_)) {
  config_->LoadConfig();
}

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
  // std::scoped_lock rwlock(mutex_);
  std::unique_lock rwlock(mutex_);
  if (immutable_memtables_.size() >= config_->GetMaxImmuMemTablesInMem()) {
    cv_.wait(rwlock, [this](){ 
      return immutable_memtables_.size() == 0;
    });
  }

  memtable_->Put(key, value, txn_id);

  if (memtable_->GetMemTableSize() >= config_->GetPerMemTableSizeLimit()) {
    immutable_memtables_.push_back(std::move(memtable_));

    if (immutable_memtables_.size() >= config_->GetMaxImmuMemTablesInMem()) {
      // Flush thread to flush memtable to disk
      // TODO(namnh) : CRASH!!!. When number of write ops is high, an immutable
      // memtable can be flushed to disk multiple time => double free
      // flushing_sequence_number_++;
      for (const auto &immu_basememtable : immutable_memtables_) {
        auto* immutable_memtable = static_cast<MemTable*>(immu_basememtable.get());
        // if (immutable_memtable->GetSequenceNumber() != 0) {
        //   continue;
        // }


        // immutable_memtable->SetSequenceNumber(flushing_sequence_number_);
        // flushing_memtables_.push_back(immu_basememtable.get());
      }
      thread_pool_->Enqueue(
        &DBImpl::FlushMemTableJob, this, flushing_sequence_number_.load());
    }

    // Create new empty mutable memtable
    memtable_ = std::make_unique<MemTable>();
  }
}

bool DBImpl::ShouldFlushMemTable() {
    int total_flush_memtable = 
        std::count_if(immutable_memtables_.begin(),
                      immutable_memtables_.end(),
                      [this](const auto& elem) { 
                        return (static_cast<const MemTable*>(elem.get()))->GetSequenceNumber() == flushing_sequence_number_;
                      });

  return (total_flush_memtable == config_->GetMaxImmuMemTablesInMem()) ? true : false;
}

// void DBImpl::FlushMemTableJob(const BaseMemTable *const immutable_memtable) {
void DBImpl::FlushMemTableJob(const uint64_t flush_sequence_number) {
  // Key point: Db DOES NOT need to acquire mutex here. Because
  // CreateLatestVersion is protected by mutex. So, at a time, there is always 1
  // thread/process can access. It means that, each latest version returned is
  // ensured to be race condition free

  Version *latest_version = version_manager_->CreateLatestVersion();
  if (!latest_version) {
    return;
  }

  std::vector<const BaseMemTable*> flush_memtables_list_;
  for (const auto& flushing_base_memtable : flushing_memtables_) {
    auto* flushing_memtable = static_cast<const MemTable*>(flushing_base_memtable);
    // if (flushing_memtable->GetSequenceNumber() != flush_sequence_number) {
      // continue;
    // }

    // flush_memtables_list_.push_back(flushing_base_memtable);
  }

  if (!latest_version->CreateNewSST(immutable_memtables_)) {
    return;
  }
  
  // if (!latest_version->CreateNewSST(immutable_memtable)) {
  //   return;
  // }

  // TODO(namnh) : Update manifest info
  // After sst is persisted to disk and manifest is updated, remove immutable
  // memtable from memory
  {
    std::scoped_lock rwlock(mutex_);
    // immutable_memtables_.erase(
    //     std::remove_if(immutable_memtables_.begin(), immutable_memtables_.end(),
    //                    [&flush_memtables_list_, flush_sequence_number](const std::unique_ptr<BaseMemTable> &ptr) {
    //                     //  return ptr.get() == immutable_memtable;
    //                     return std::any_of(flush_memtables_list_.begin(),
    //                                        flush_memtables_list_.end(),
    //                                         [&](const BaseMemTable* ref) {return ref == ptr.get()/* && (static_cast<const MemTable*>(ref))->GetSequenceNumber() == flush_sequence_number*/;});
    //                    }),
    //     immutable_memtables_.end());

    immutable_memtables_.clear();
    cv_.notify_one();
  }
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