#include "db/transaction_manager.h"

#include "db/transcation.h"

#include <cassert>

namespace kvs {

Transaction *TransactionManager::CreateNewTransaction() {
  // TODO(namnh) : Do we need to lock when creating new transaction.
  // std::scoped_lock<std::mutex> lock(mutex_);
  TxnId new_txn_id = GetNextTransactionId();
  auto new_txn =
      std::make_unique<Transaction>(this, new_txn_id, GetIsolationLevel());
  
  std::scoped_lock<std::mutex> lock(mutex_);
  bool is_inserted = txns_.insert({new_txn_id, std::move(new_txn)});

  return txns_[new_txn_id].get();
}

TxnId TransactionManager::GetNextTransactionId() {
  return next_txn_id_.fetch_add(1);
}

IsolationLevel TransactionManager::GetIsolationLevel() {
  std::scoped_lock<std::mutex> lock(mutex_);
  return isolation_level_;
}

} // namespace kvs