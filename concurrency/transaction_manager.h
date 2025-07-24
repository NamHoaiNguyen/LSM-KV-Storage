#ifndef CONCURRENCY_TRANSACTION_MANAGER_H
#define CONCURRENCY_TRANSACTION_MANAGER_H

#include "common/macros.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace kvs {

class Transaction;

class TransactionManager {
  public:
    TransactionManager() = default;

    ~TransactionManager() = default;

    void Abort(Txn_id txn_id);

    void Begin();

    void Commit(Txn_id txn_id);

  private:
    // TODO(namnh) : unique_ptr or shared_ptr
    std::unordered_map<Txn_id, std::unique_ptr<Transaction>> transactions_;

    std::atomic<uint64_t> next_txn_id_;

    // Only 1 transaction can commit at a point
    std::mutex mutex_;
};

} // namespace kvs

#endif // CONCURRENCY_TRANSACTION_MANAGER_H