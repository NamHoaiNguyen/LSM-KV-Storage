#include "concurrency/transaction_manager.h"

#include "concurrency/transaction.h"
#include "db/db.h"

#include <cassert>

namespace kvs {

TransactionManager::TransactionManager(DB *db) : db_(db) {}

Transaction *TransactionManager::CreateNewTransaction() {
  // TODO(namnh) : Do we need to lock when creating new transaction.
  // std::scoped_lock<std::mutex> lock(mutex_);
  TxnId new_txn_id = GetNextTransactionId();
  auto new_txn =
      std::make_unique<Transaction>(this, db_, new_txn_id, GetIsolationLevel());

  std::scoped_lock<std::mutex> lock(mutex_);
  bool is_inserted = txns_.insert({new_txn_id, std::move(new_txn)}).second;
  assert(is_inserted);

  return txns_[new_txn_id].get();
}

TxnId TransactionManager::GetNextTransactionId() {
  return next_txn_id_.fetch_add(1);
}

bool TransactionManager::DoesTransactionExist(TxnId txn_id) {
  return txns_.find(txn_id) != txns_.end();
}

IsolationLevel TransactionManager::GetIsolationLevel() {
  std::scoped_lock<std::mutex> lock(mutex_);
  return isolation_level_;
}

} // namespace kvs