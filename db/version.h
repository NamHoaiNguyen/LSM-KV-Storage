#ifndef DB_VERSION_H
#define DB_VERSION_H

#include "common/macros.h"

#include <deque>
#include <memory>
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
// moment
class Version {
public:
  Version(DBImpl *db, const Config *config);

  ~Version() = default;

  // No copy allowed
  Version(const Version &) = delete;
  Version &operator=(Version &) = delete;

  // Move constructor/assignment
  Version(Version &&) = default;
  Version &operator=(Version &&) = default;

  bool CreateNewSST(const BaseMemTable *const immutable_memtable);

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

    friend class Compact;

  private:
    std::unique_ptr<sstable::Table> table_;

    SSTId table_id_;

    uint8_t level_;

    std::string smallest_key_;

    std::string largest_key_;
  };

  std::vector<std::vector<std::unique_ptr<SSTInfo>>> levels_sst_info_;

  std::unique_ptr<Compact> compact_;

  // Below are objects that Version does NOT own lifetime. So, DO NOT
  // modify, including change memory that it is pointing to,
  // allocate/deallocate, etc... these objects.
  const Config *config_;

  DBImpl *db_;

  kvs::ThreadPool *thread_pool_;
};

} // namespace db

} // namespace kvs

#endif // DB_VERSION_H