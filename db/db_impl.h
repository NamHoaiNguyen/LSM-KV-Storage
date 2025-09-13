#ifndef DB_LSM_H
#define DB_LSM_H

#include "common/macros.h"
#include "db/version.h"
#include "db/version_edit.h"

#include <condition_variable>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace kvs {

class ThreadPool;

namespace mvcc {
class TransactionManager;
} // namespace mvcc

namespace sstable {
class Table;
} // namespace sstable

namespace db {

class BaseIterator;
class BaseMemTable;
class Config;
class VersionManager;

class DBImpl {
public:
  explicit DBImpl(bool is_testing = false);

  ~DBImpl();

  // No copy allowed
  DBImpl(const DBImpl &) = delete;
  DBImpl &operator=(const DBImpl &) = delete;

  // No move allowed
  DBImpl(DBImpl &&) = default;
  DBImpl &operator=(DBImpl &&) = default;

  std::optional<std::string> Get(std::string_view key, TxnId txn_id);

  void Put(std::string_view key, std::string_view value, TxnId txn_id);

  uint64_t GetNextSSTId();

  void LoadDB();

  const Config *const GetConfig();

  const VersionManager *GetVersionManager();

  void ForceFlushMemTable();

  friend class Compact;

  // For testing
  const BaseMemTable *GetCurrentMemtable();

  const std::vector<std::unique_ptr<BaseMemTable>> &GetImmutableMemTables();

private:
  void FlushMemTableJob();

  void CreateNewSST(const std::unique_ptr<BaseMemTable> &immutable_memtable,
                    std::unique_ptr<VersionEdit> &version_edit,
                    std::latch &work_done);

  void TriggerCompaction();

  const std::string dbname_;

  std::atomic<uint64_t> next_sstable_id_;

  struct PairHash {
    std::size_t
    operator()(const std::pair<uint64_t, uint8_t> &p) const noexcept {
      // Shift the small 8-bit value into the lower bits
      return std::hash<uint64_t>()((p.first << 8) | p.second);
    }
  };

  std::unique_ptr<BaseMemTable> memtable_;

  std::vector<std::unique_ptr<BaseMemTable>> immutable_memtables_;

  std::vector<const BaseMemTable *> flushing_memtables_;

  std::unique_ptr<mvcc::TransactionManager> txn_manager_;

  // Threadppol ISN'T COPYABLE AND MOVEABLE
  // So, we must allocate/deallocate by ourselves
  kvs::ThreadPool *thread_pool_;

  std::unique_ptr<Config> config_;

  std::unique_ptr<VersionManager> version_manager_;

  // Mutex to protect some critical data structures
  // (immutable_memtables_ list, levels_sst_info_)
  // std::shared_mutex immutable_memtables_mutex_;
  std::shared_mutex mutex_;

  std::condition_variable_any cv_;
};

} // namespace db

} // namespace kvs

#endif // DB_LSM_H