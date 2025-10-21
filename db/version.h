#ifndef DB_VERSION_H
#define DB_VERSION_H

#include "common/macros.h"
#include "db/status.h"
#include "db/version_edit.h"

#include <atomic>
#include <cassert>
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
class DBImpl;
class ThreadPool;
class VersionEdit;
class VersionManager;

// Version is a snapshot holding info of all SST files at the specific
// moment.
// Each version has it owns compaction. This design avoid compaction process
// must acquire mutex for lists(or other data structures) which hold infomation
// of SSTs that needed to be compacted.

// NOTE: A version is IMMUTABLE snapshot after be built. In other worlds, DO NOT
// execute methods that changing data of version
// TODO(namnh) : have any other designs for this ?

class Version {
public:
  Version(uint64_t version_id, int num_sst_levels,
          kvs::ThreadPool *thread_pool, const DBImpl* db);

  ~Version() = default;

  // No copy allowed
  Version(const Version &) = delete;
  Version &operator=(Version &) = delete;

  // Move constructor/assignment
  Version(Version &&other) = delete;
  Version &operator=(Version &&other) = delete;

  void IncreaseRefCount() const;

  void DecreaseRefCount() const;

  // Get Key from version
  GetStatus Get(std::string_view key, TxnId txn_id) const;

  bool NeedCompaction() const;

  std::optional<int> GetLevelToCompact() const;

  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &
  GetImmutableSSTMetadata() const;

  // ALL NON-CONST methods are only called when building new version
  std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &GetSSTMetadata();

  const std::vector<double> &GetImmutableLevelsScore() const;

  // ALL NON-CONST methods  are only called when building new version
  std::vector<double> &GetLevelsScore();

  size_t GetNumberSSTFilesAtLevel(int level) const;

  uint64_t GetVersionId() const;

  uint64_t GetRefCount() const;

  friend class Compact;

  // For testing
  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &
  GetSSTMetadata() const;

private:
  std::shared_ptr<SSTMetadata> FindFilesAtLevel(int level,
                                                std::string_view key) const;
  const uint64_t version_id_;

  std::vector<std::vector<std::shared_ptr<SSTMetadata>>> levels_sst_info_;

  // Level that need to compact
  uint8_t compaction_level_;

  uint64_t compaction_score_;

  std::vector<double> levels_score_;

  mutable std::atomic<uint64_t> ref_count_;

  // Below are objects that Version does NOT own lifetime. So, DO NOT
  // modify, including change memory that it is pointing to,
  // allocate/deallocate, etc... these objects.
  const Config *config_;

  kvs::ThreadPool *thread_pool_;

  const VersionManager *version_manager_;

  const sstable::BlockReaderCache* block_reader_cache_;

  const sstable::TableReaderCache* table_reader_cache_;
};

} // namespace db

} // namespace kvs

#endif // DB_VERSION_H