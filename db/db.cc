#include "db/db.h"

#include "concurrency/transaction_manager.h"
#include "db/lsm.h"

namespace kvs {

DB::DB() :
  txn_manager_(std::make_unique<TransactionManager>()),
  lsm_(std::make_unique<LSM>()) {}

DB::~DB() = default;

Transaction* DB::Begin() {
  if (!lsm_  || !txn_manager_) {
    std::exit(EXIT_FAILURE);
  }

  Transaction* new_txn = txn_manager_->CreateNewTransaction();
  return new_txn;
}

void DB::Get(std::string_view key, TxnId txn_id) {
  // TODO(namnh) : Use concrete type.
  auto value = lsm_->get(key, txn_id);
  
}

} // namespace kvs