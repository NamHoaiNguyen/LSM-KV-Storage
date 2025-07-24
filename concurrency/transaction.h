#ifndef CONCURRENCY_TRANSACTION__H
#define CONCURRENCY_TRANSACTION__H

#include "common/macros.h"

#include <mutex>
#include <unordered_map>

namespace kvs {

enum class IsolationLevel {
  READ_UNCOMMITED,

  READ_COMMITED,

  REPEATABLE_READ,

  SERIALIZABLE,
};

enum class TransactionState {
  ABORTED,

  COMMITED,

  RUNNING,
};

class Transaction {
  public:
    Transaction() = default;

    ~Transaction() = default;

  private:
    Txn_id txn_id_;

    // Timestamp when transaction "BEGIN"
    TimeStamp read_timestamp_;

    // Timestamp when transaction "COMMIT"
    TimeStamp commit_timestamp_;
};

} // namespace kvs

#endif // CONCURRENCY_TRANSACTION__H