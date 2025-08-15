#include "db/lsm.h"

#include "common/base_iterator.h"
#include "concurrency/transaction_manager.h"
#include "concurrency/transaction.h"
#include "db/memtable_iterator.h"
#include "db/memtable.h"

namespace kvs {

LSM::LSM() = default;

LSM::~LSM() = default;

// std::unique_ptr<BaseIterator> LSM::CreateNewIterator() {
//   return std::make_unique<MemTableIterator>();
// }

std::optional<std::string> LSM::Get(std::string_view key, TxnId txn_id) {
  std::optional<std::string> value;

  // Find data from Memtable first
  // value = memtable_->Get(key);
  // if (value.has_value()) {
  //   return value;
  // }

  // Load data from SSTable
  return std::nullopt;
}

} // namespace kvs