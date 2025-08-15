#ifndef DB_LSM_H
#define DB_LSM_H

#include "common/macros.h"

#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string_view>
#include <thread>
#include <vector>

namespace kvs {

class BaseIterator;
class MemTable;
class SSTable;
class TransactionManager;
class Transaction;

using BackgroundFlushJob = std::function<void()>;

class LSM {
public:
  LSM();

  ~LSM();

  // Copy constructor and assignment
  LSM(const LSM &) = delete;
  LSM &operator=(const LSM &) = delete;

  // Move constructor and assigment
  // TODO(namnh) : Recheck this one.
  LSM(LSM &&) = default;
  LSM &operator=(LSM &&) = default;

  std::optional<std::string> Get(std::string_view key, TxnId txn_id);

  std::unique_ptr<BaseIterator> CreateNewIterator();

private:
  std::unique_ptr<TransactionManager> txn_manager_;

  // TODO(namnh) : unique_ptr or shared_ptr
  std::unique_ptr<MemTable> memtable_;

  std::vector<std::unique_ptr<MemTable>> immutable_memtables_;

  std::shared_mutex memtable_mutex_;

  std::thread flush_job_;
};

} // namespace kvs

#endif // DB_LSM_H