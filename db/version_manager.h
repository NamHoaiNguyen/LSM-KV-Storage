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
  VersionManager(const DBImpl *db, const kvs::ThreadPool *const thread_pool);

  ~VersionManager() = default;

  // No copy allowed
  VersionManager(const VersionManager &) = delete;
  VersionManager &operator=(VersionManager &) = delete;

  // Move constructor/assignment
  VersionManager(VersionManager &&) = default;
  VersionManager &operator=(VersionManager &&) = default;

  void RemoveObsoleteVersion(uint64_t version_id) const;

  // Create latest version and apply new SSTs metadata
  void ApplyNewChanges(std::unique_ptr<VersionEdit> version_edit);

  bool NeedSSTCompaction() const;

  const std::unordered_map<uint64_t, std::unique_ptr<Version>> &
  GetVersions() const;

  // For testing
  const Version *GetLatestVersion() const;

  const Config *const GetConfig();

private:
  void InitVersionWhenLoadingDb(std::unique_ptr<VersionEdit> version_edit);

  void CreateNewVersion(std::unique_ptr<VersionEdit> version_edit);

  std::atomic<uint64_t> next_version_id_{0};

  // std::deque<std::unique_ptr<Version>> versions_;
  mutable std::unordered_map<uint64_t, std::unique_ptr<Version>> versions_;

  std::unique_ptr<Version> latest_version_;

  // Below are objects that VersionManager does NOT own lifetime. So, DO NOT
  // modify, including change memory that it is pointing to,
  // allocate/deallocate, etc... these objects.
  const DBImpl *db_;

  const Config *config_;

  const kvs::ThreadPool *const thread_pool_;

  mutable std::mutex mutex_;
};

} // namespace db

} // namespace kvs

#endif // DB_VERSION_MANAGER_H