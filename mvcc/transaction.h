#ifndef CONCURRENCY_TRANSACTION__H
#define CONCURRENCY_TRANSACTION__H

#include "common/macros.h"
#include "mvcc/transaction_manager.h"

#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace kvs {

namespace db {
class DBImpl;
} // namespace db

namespace mvcc {

enum class TransactionState {
  ABORTED,

  COMMITED,

  RUNNING,
};

class Transaction {
public:
  Transaction(TransactionManager *txn_manager, db::DBImpl *db, TxnId txn_id,
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

  db::DBImpl *db_;
};

} // namespace mvcc

} // namespace kvs

#endif // CONCURRENCY_TRANSACTION__H