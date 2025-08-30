#include "db/db_impl.h"

#include "common/base_iterator.h"
#include "common/thread_pool.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "db/compact.h"
#include "db/memtable.h"
#include "db/memtable_iterator.h"
#include "db/status.h"
#include "io/base_file.h"
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

DBImpl::DBImpl(std::string_view dbname)
    : dbname_(std::string(dbname)), next_sstable_id_(0),
      memtable_(std::make_unique<MemTable>()),
      compact_(std::make_unique<Compact>(this)),
      txn_manager_(std::make_unique<TransactionManager>(this)),
      current_L0_files_(0), thread_pool_(new ThreadPool()) {}

DBImpl::~DBImpl() { delete thread_pool_; }

std::optional<std::string> DBImpl::Get(std::string_view key, TxnId txn_id) {
  GetStatus status;

  // Find data from Memtable
  status = memtable_->Get(key, txn_id);
  if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
    return status.value;
  }

  // If key is not found, continue finding from immutable memtables
  {
    std::shared_lock<std::shared_mutex> rlock(mutex_);
    for (const auto &immu_memtable :
         immutable_memtables_ | std::views::reverse) {
      status = immu_memtable->Get(key, txn_id);
      if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
        return status.value;
      }
    }
  }

  // TODO(namnh) : Find from sstable
  return std::nullopt;
}

void DBImpl::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  memtable_->Put(key, value, txn_id);

  if (memtable_->GetMemTableSize() >= kDebugMemTableSize) {
    // memtable_->SetImmutable();
    size_t immutable_memtables_size;
    {
      std::scoped_lock rwlock(mutex_);
      immutable_memtables_.push_back(std::move(memtable_));
      immutable_memtables_size = immutable_memtables_.size();

      if (immutable_memtables_size >= kDebugMaxNumberImmutableTablesInMemory) {
        // Flush thread to flush memtable to disk
        for (int i = 0; i < immutable_memtables_.size(); i++) {
          thread_pool_->Enqueue(&DBImpl::FlushMemTableJob, this, i);
        }
      }
    }

    // Create new empty mutable memtable
    memtable_ = std::make_unique<MemTable>();
  }
}

void DBImpl::FlushMemTableJob(int immutable_memtable_index) {
  // Create new level 0 SST
  std::unique_ptr<Table> new_sst = CreateNewSST(immutable_memtable_index);
  auto new_sst_info = std::make_unique<SSTInfo>(std::move(new_sst));

  {
    // Need to acquire lock when moving data from immutable into buffer
    std::scoped_lock rwlock(mutex_);
    immutable_memtables_.erase(immutable_memtables_.begin() +
                               immutable_memtable_index);
    levels_sst_info_[0].push_back(std::move(new_sst_info));

    if (current_L0_files_ >= kL0_CompactionTrigger) {
      thread_pool_->Enqueue(&Compact::PickCompact, compact_.get(),
                            levels_sst_info_[0].size());
    }
  }
}

std::unique_ptr<Table> DBImpl::CreateNewSST(int immutable_memtable_index) {
  std::string next_sst = std::to_string(next_sstable_id_.fetch_add(1));
  std::string filename = path + next_sst + ".sst";
  auto sst = std::make_unique<Table>(std::move(filename));
  if (!sst->Open()) {
    throw std::runtime_error("Can't create sstable file");
  }

  // TODO(namnh) : unique_ptr or shared_ptr?
  const BaseMemTable *immutable_memtable =
      immutable_memtables_.at(immutable_memtable_index).get();
  auto iterator = std::make_unique<MemTableIterator>(immutable_memtable);

  // Iterate through all key/value pairs to add them to sst
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    sst->AddEntry(iterator->GetKey(), iterator->GetValue(),
                  iterator->GetTransactionId(), iterator->GetType());
  }

  // All of key/value pairs had been written to sst. SST should be flushed to
  // disk
  sst->Finish();

  current_L0_files_++;

  return sst;
}

// SST INFO
DBImpl::SSTInfo::SSTInfo(std::unique_ptr<Table> table)
    : table_(std::move(table)) {}

} // namespace kvs