#include "db/transaction.h"

#include "db/engine.h"
#include "db/transaction_manager.h"

namespace kvs
{
Transaction::Transaction(
    TransactionManager *txn_manager, Engine *engine, TxnId txn_id)
    : txn_manager_(txn_manager), engine_(engine), txn_id_(txn_id) {}

void Transaction::Abort() {}

void Transaction::Commit() {
  if (!engine || !txn_manager_) {
    std::exit(EXIT_FAILURE);
  }
}

std::optional<std::string> Transaction::Get(std::string_view key) {
  if (!engine || !txn_manager_) {
    std::exit(EXIT_FAILURE);
  }

  if (txn_manager_.find(txn_id_) == txn_manager_.end()) {
    throw std::runtime_error("Transaction not befound");
  }

  std::optional<std::string> value;
  value = engine_->Get(key, txn_id_);

  return value;
}

void Transaction::Put(std::string_view key) {}

} // namespace kvs
