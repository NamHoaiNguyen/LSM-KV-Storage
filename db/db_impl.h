#ifndef DB_LSM_H
#define DB_LSM_H

#include "common/macros.h"

#include <condition_variable>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string_view>
#include <thread>
#include <vector>

namespace kvs {

class BaseIterator;
class BaseMemtable;
class SSTable;
class ThreadPool;
class TransactionManager;

class DBImpl {
public:
  DBImpl();

  ~DBImpl();

  // No copy allowed
  DBImpl(const DBImpl &) = delete;
  DBImpl &operator=(const DBImpl &) = delete;

  // No move allowed
  DBImpl(DBImpl &&) = delete;
  DBImpl &operator=(DBImpl &&) = delete;

  std::optional<std::string> Get(std::string_view key, TxnId txn_id);

  void Put(std::string_view key, std::string_view value, TxnId txn_id);

  std::unique_ptr<BaseIterator> CreateNewIterator();

private:
  void FlushMemTableJob(int immutable_memtable_index);

  std::unique_ptr<TransactionManager> txn_manager_;

  std::unique_ptr<BaseMemtable> memtable_;

  std::vector<std::unique_ptr<BaseMemtable>> immutable_memtables_;

  std::shared_mutex immutable_memtables_mutex_;

  // Threadppol ISN'T COPYABLE AND MOVEABLE
  // So, we must allocate/deallocate by ourselves
  ThreadPool *thread_pool_;
};

} // namespace kvs

#endif // DB_LSM_H