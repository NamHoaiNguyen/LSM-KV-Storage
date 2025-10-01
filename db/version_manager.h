#ifndef DB_VERSION_MANAGER_H
#define DB_VERSION_MANAGER_H

#include "db/version.h"

#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace kvs {

class ThreadPool;

namespace sstable {
class BlockReaderCache;
class TableReaderCache;
} // namespace sstable

namespace db {

class BaseMemTable;
class DBImpl;
class Compact;
class Config;

class VersionManager {
public:
  VersionManager(const DBImpl *db, const kvs::ThreadPool *thread_pool);

  ~VersionManager() = default;

  // No copy allowed
  VersionManager(const VersionManager &) = delete;
  VersionManager &operator=(VersionManager &) = delete;

  // Move constructor/assignment
  VersionManager(VersionManager &&) = default;
  VersionManager &operator=(VersionManager &&) = default;

  void RemoveObsoleteVersion(uint64_t version_id);

  // Create latest version and apply new SSTs metadata
  void ApplyNewChanges(std::unique_ptr<VersionEdit> version_edit);

  bool NeedSSTCompaction() const;

  GetStatus GetKey(std::string_view key, TxnId txn_id, SSTId table_id,
                   uint64_t file_size) const;

  const std::unordered_map<uint64_t, std::unique_ptr<Version>> &
  GetVersions() const;

  // For testing
  const Version *GetLatestVersion() const;

  const Config *const GetConfig();

private:
  void InitVersionWhenLoadingDb(std::unique_ptr<VersionEdit> version_edit);

  void CreateNewVersion(std::unique_ptr<VersionEdit> version_edit);

  // TODO(namnh) : we need a ref-count mechanism to delist version that isn't
  // referenced to anymore
  std::atomic<uint64_t> next_version_id_{0};

  // std::deque<std::unique_ptr<Version>> versions_;
  std::unordered_map<uint64_t, std::unique_ptr<Version>> versions_;

  std::unique_ptr<Version> latest_version_;

  // Below are objects that VersionManager does NOT own lifetime. So, DO NOT
  // modify, including change memory that it is pointing to,
  // allocate/deallocate, etc... these objects.
  const DBImpl *db_;

  const sstable::TableReaderCache *table_reader_cache_;

  const sstable::BlockReaderCache *block_reader_cache_;

  const Config *config_;

  const kvs::ThreadPool *thread_pool_;

  mutable std::mutex mutex_;
};

} // namespace db

} // namespace kvs

#endif // DB_VERSION_MANAGER_H