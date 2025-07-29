#include "db/transaction_manager.h"

#include "db/transcation.h"

#include <cassert>

namespace kvs {

Transaction *TransactionManager::CreateNewTransaction() {
  std::scoped_lock<std::mutex> lock(mutex_);
  TxnId new_txn_id = GetNextTransactionId();
  auto new_txn =
      std::make_unique<Transaction>(this, new_txn_id, GetIsolationLevel());

  bool is_inserted = txns_.insert({new_txn_id, std::move(new_txn)});
  assert(is_inserted);

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