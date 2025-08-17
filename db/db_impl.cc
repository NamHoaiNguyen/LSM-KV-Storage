#include "db/db_impl.h"

#include "common/base_iterator.h"
#include "common/thread_pool.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "db/memtable.h"
#include "db/memtable_iterator.h"

namespace {

// TODO(namnh) : dynamic
constexpr size_t kMemTableSize = 4 * 1024 * 1024; // MB
constexpr size_t kDebugMemTableSize = 5;          // Bytes

// TODO(namnh) : dynamic(Yeah, I want to challenge myself!!!)
constexpr size_t kMaxNumberImmutableTablesInMemory = 3;
constexpr size_t kDebugMaxNumberImmutableTablesInMemory = 1;

} // namespace

namespace kvs {

DBImpl::DBImpl()
    : memtable_(std::make_unique<MemTable>()), thread_pool_(new ThreadPool()) {}

DBImpl::~DBImpl() { delete thread_pool_; }

std::optional<std::string> DBImpl::Get(std::string_view key, TxnId txn_id) {
  std::optional<std::string> value;

  // Find data from Memtable
  value = memtable_->Get(key, txn_id);
  if (value.has_value()) {
    return value;
  }

  // Continue finding from immutable memtables
  {
    // TODO(namnh) : holding lock when traversing immutable memtables list
    // is quite expensive. Should find another approach.
    std::shared_lock<std::shared_mutex> rlock_immutable_memtables_(
        immutable_memtables_mutex_);
    for (const auto &immu_memtable : immutable_memtables_) {
      value = immu_memtable->Get(key, txn_id);
      if (value.has_value()) {
        return value;
      }
    }
  }

  // TODO(namnh) : Find from sstable
  return value;
}

void DBImpl::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  memtable_->Put(key, value, txn_id);

  if (memtable_->GetMemTableSize() >= kDebugMemTableSize) {
    memtable_->SetImmutable();
    size_t immutable_memtables_size;
    {
      std::scoped_lock rwlock_immutable_memtables(immutable_memtables_mutex_);
      immutable_memtables_.push_back(std::move(memtable_));
      immutable_memtables_size = immutable_memtables_.size();
    }
    if (immutable_memtables_size >= kDebugMaxNumberImmutableTablesInMemory) {
      // Flush thread to flush memtable to disk
      for (int i = 0; i < immutable_memtables_.size(); i++) {
        thread_pool_->Enqueue(&DBImpl::FlushMemTableJob, this, i);
      }
    }

    memtable_ = std::make_unique<MemTable>();
  }
}

} // namespace kvs