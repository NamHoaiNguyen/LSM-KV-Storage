#include "transaction_manager.h"

#include "transcation.h"

namespace kvs {

void TransactionManager::Abort(Txn_id txn_id) {}

void TransactionManager::Begin() {
  std::scoped_lock<std::mutex> lock(mutex_);
}

void TransactionManager::Commit(Txn_id txn_id) {}

} // namespace kvs