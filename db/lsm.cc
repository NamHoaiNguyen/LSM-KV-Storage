#include "db/lsm.h"

namespace kvs {

LSM::LSM() {}

std::optional<std::string> LSM::Get(std::string_view key) {
  std::optional<std::string> value;

  // Find data from Memtable first
  value = memtable_->Get(key);
  if (value.has_value()) {
    return value;
  }
  
  // Load data from SSTable
}

} // namespace kvs