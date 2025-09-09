#ifndef DB_VERSION_H
#define DB_VERSION_H

#include "common/macros.h"
#include "db/status.h"

#include <atomic>
#include <deque>
#include <latch>
#include <memory>
#include <ranges>
#include <vector>

namespace kvs {

class ThreadPool;

namespace sstable {
class Table;
} // namespace sstable

namespace db {

class BaseMemTable;
class Compact;
class Config;
class DBImpl;

// Version is a snapshot holding info of all SST files at the specific
// moment.
// Each version has it owns compaction. This design avoid compaction process
// must acquire mutex for lists(or other data structures) which hold infomation
// of SSTs that needed to be compacted.
class Version {
public:
  struct SSTInfo {
  public:
    SSTInfo();
    SSTInfo(std::shared_ptr<sstable::Table> table);

    ~SSTInfo() = default;

    // No copy allowed
    SSTInfo(const SSTInfo &) = delete;
    SSTInfo &operator=(SSTInfo &) = delete;

    SSTInfo(SSTInfo &&) = default;
    SSTInfo &operator=(SSTInfo &&) = default;

    friend class Compact;

    // private:
    // std::unique_ptr<sstable::Table> table_;
    std::shared_ptr<sstable::Table> table_;

    SSTId table_id_;

    uint8_t level_;

    std::atomic<bool> should_be_deleted_;

    std::string smallest_key_;

    std::string largest_key_;
  };

  Version(DBImpl *db, const Config *config, ThreadPool *thread_pool);

  ~Version() = default;

  // No copy allowed
  Version(const Version &) = delete;
  Version &operator=(Version &) = delete;

  // Move constructor/assignment
  Version(Version &&) = default;
  Version &operator=(Version &&) = default;

  GetStatus Get(std::string_view key, TxnId txn_id) const;

  void CreateNewSSTs(
      const std::vector<std::unique_ptr<BaseMemTable>> &immutable_memtables);

  const std::vector<std::vector<std::shared_ptr<SSTInfo>>> &
  GetImmutableSSTInfo() const;
  std::vector<std::vector<std::shared_ptr<SSTInfo>>> &GetSSTInfo();

  friend class Compact;

private:
  void CreateNewSST(const std::unique_ptr<BaseMemTable> &immutable_memtables,
                    std::latch &work_done);

  // TODO(namnh) : do I need to protect this one ?
  // TODO(namnh) : How to construct this data structure ?
  // If using unique_ptr, each version has its own data SST info.
  // In other words, SST info of each version is immutable for other versions.
  // Whereas, if using shared_ptr, there MUST be a lock mechanism to protect
  // SSTinfo data structure
  // std::vector<std::vector<std::unique_ptr<SSTInfo>>> levels_sst_info_;
  std::vector<std::vector<std::shared_ptr<SSTInfo>>> levels_sst_info_;

  std::unique_ptr<Compact> compact_;

  // Below are objects that Version does NOT own lifetime. So, DO NOT
  // modify, including change memory that it is pointing to,
  // allocate/deallocate, etc... these objects.
  const Config *config_;

  DBImpl *db_;

  kvs::ThreadPool *thread_pool_;

  std::mutex mutex_;
};

} // namespace db

} // namespace kvs

#endif // DB_VERSION_H