#include "mvcc/transaction.h"

#include "db/db_impl.h"
#include "mvcc/transaction_manager.h"

namespace kvs {

namespace mvcc {

Transaction::Transaction(TransactionManager *txn_manager, db::DBImpl *db,
                         TxnId txn_id, IsolationLevel isolation_level)
    : txn_manager_(txn_manager), db_(db), txn_id_(txn_id),
      isolation_level_(isolation_level) {}

void Transaction::Abort() {}

void Transaction::Commit() {
  if (!db_ || !txn_manager_) {
    std::exit(EXIT_FAILURE);
  }
}

std::optional<std::string> Transaction::Get(std::string_view key) {
  if (!db_ || !txn_manager_) {
    std::exit(EXIT_FAILURE);
  }

  if (txn_manager_->DoesTransactionExist(txn_id_)) {
    throw std::runtime_error("Transaction not be found");
  }

  // std::optional<std::string> value;
  // value = db_->Get(key, txn_id_);

  return std::nullopt;
}

void Transaction::Put(std::string_view key) {}
// If isolation_level_ == READ_UNCOMMITED,
// write direcly to memtable

} // namespace mvcc

} // namespace kvs
