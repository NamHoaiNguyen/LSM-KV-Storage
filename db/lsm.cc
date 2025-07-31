#include "db/lsm.h"

#include "db/memtable.h"

namespace kvs {

LSM::LSM() = default;

LSM::~LSM() = default;

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