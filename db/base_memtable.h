#ifndef DB_BASE_MEMTABLE_H
#define DB_BASE_MEMTABLE_H

#include "common/macros.h"

#include <optional>
#include <string_view>
#include <vector>

namespace kvs {

class BaseMemtable {
public:
  virtual ~BaseMemtable() = default;

  virtual void BatchGet(std::vector<std::string_view> keys) = 0;

  virtual std::optional<std::string> Get(std::string_view key,
                                         TxnId txn_id) = 0;

  virtual void BatchPut(std::vector<std::string_view> keys) = 0;

  virtual void Put(std::string_view key, std::string_view value,
                   TxnId txn_id) = 0;

  virtual bool Delete(std::string_view key, TxnId txn_id) = 0;

private:
};

} // namespace kvs

#endif // DB_BASE_MEMTABLE_H