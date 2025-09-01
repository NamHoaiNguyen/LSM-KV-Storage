#ifndef DB_LSM_H
#define DB_LSM_H

#include "common/macros.h"

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
  explicit DBImpl(std::string_view dbname);

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

  friend class Compact;

private:
  class SSTInfo {
  public:
    SSTInfo() = default;
    SSTInfo(std::unique_ptr<sstable::Table> table);

    ~SSTInfo() = default;

    // No copy allowed
    SSTInfo(const SSTInfo &) = delete;
    SSTInfo &operator=(SSTInfo &) = delete;

    SSTInfo(SSTInfo &&) = default;
    SSTInfo &operator=(SSTInfo &&) = default;

    friend class DBImpl;
    friend class Compact;

  private:
    std::unique_ptr<sstable::Table> table_;

    SSTId table_id_;

    uint8_t level_;

    std::string smallest_key_;

    std::string largest_key_;
  };

  void FlushMemTableJob(const BaseMemTable *const immutable_memtable);

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

  std::unique_ptr<mvcc::TransactionManager> txn_manager_;

  // Threadppol ISN'T COPYABLE AND MOVEABLE
  // So, we must allocate/deallocate by ourselves
  kvs::ThreadPool *thread_pool_;

  std::unique_ptr<Config> config_;

  std::unique_ptr<VersionManager> version_manager_;

  // To know that a SST belongs to which level
  std::vector<std::vector<std::unique_ptr<SSTInfo>>> levels_sst_info_;

  // Mutex to protect some critical data structures
  // (immutable_memtables_ list, levels_sst_info_)
  // std::shared_mutex immutable_memtables_mutex_;
  std::shared_mutex mutex_;
};

} // namespace db

} // namespace kvs

#endif // DB_LSM_H