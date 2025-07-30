#ifndef CONCURRENCY_TRANSACTION__H
#define CONCURRENCY_TRANSACTION__H

#include "common/macros.h"
#include "concurrency/transaction_manager.h"

#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace kvs {

class DB;

enum class TransactionState {
  ABORTED,

  COMMITED,

  RUNNING,
};

class Transaction {
public:
  Transaction(TransactionManager *txn_manager, DB *db, TxnId txn_id,
              IsolationLevel isolation_level);

  ~Transaction() = default;

  void Abort();

  void Begin();

  void Commit();

  std::optional<std::string> Get(std::string_view key);

  void Put(std::string_view key);

private:
  IsolationLevel isolation_level_;

  TxnId txn_id_;

  // Timestamp when transaction "BEGIN"
  TimeStamp read_timestamp_;

  // Timestamp when transaction "COMMIT"
  TimeStamp commit_timestamp_;

  TransactionManager *txn_manager_;

  DB *db_;
};

} // namespace kvs

#endif // CONCURRENCY_TRANSACTION__H