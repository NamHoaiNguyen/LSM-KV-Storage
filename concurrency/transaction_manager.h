#ifndef CONCURRENCY_TRANSACTION_MANAGER_H
#define CONCURRENCY_TRANSACTION_MANAGER_H

#include "common/macros.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace kvs {

class DBImpl;
class Transaction;

enum class IsolationLevel {
  READ_UNCOMMITED,

  READ_COMMITED,

  REPEATABLE_READ,

  SERIALIZABLE,
};

class TransactionManager {
public:
  TransactionManager(DBImpl *db);

  ~TransactionManager() = default;

  // No copy allowed
  TransactionManager(const TransactionManager &) = delete;
  TransactionManager &operator=(TransactionManager &) = delete;

  // Move constructor/assignment
  TransactionManager(TransactionManager &&) = default;
  TransactionManager &operator=(TransactionManager &&) = default;

  Transaction *CreateNewTransaction();

  TxnId GetNextTransactionId();

  IsolationLevel GetIsolationLevel();

  bool DoesTransactionExist(TxnId txn_id);

private:
  IsolationLevel isolation_level_;

  // TODO(namnh) : unique_ptr or shared_ptr
  std::unordered_map<TxnId, std::unique_ptr<Transaction>> txns_;

  DBImpl *db_;

  std::atomic<TxnId> next_txn_id_;

  // Only 1 transaction can commit at a point
  std::mutex mutex_;
};

} // namespace kvs

#endif // CONCURRENCY_TRANSACTION_MANAGER_H