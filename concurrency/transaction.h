#ifndef CONCURRENCY_TRANSACTION__H
#define CONCURRENCY_TRANSACTION__H

#include "common/macros.h"

#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace kvs {

class Engine;
class TransactionManager;

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
    Transaction(TransactionManager* txn_manager, Engine* engine, TxnId txn_id)

    ~Transaction() = default;

    void Abort();

    void Begin();

    void Commit();

    std::optional<std::string> Get(std::string_view key);

    void Put(std::string_view key);

  private:
    Txn_id txn_id_;

    // Timestamp when transaction "BEGIN"
    TimeStamp read_timestamp_;

    // Timestamp when transaction "COMMIT"
    TimeStamp commit_timestamp_;

    TranscationManager* txn_manager_;

    Engine* engine_;
};

} // namespace kvs

#endif // CONCURRENCY_TRANSACTION__H