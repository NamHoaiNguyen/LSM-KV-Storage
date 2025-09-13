#ifndef DB_VERSION_MANAGER_H
#define DB_VERSION_MANAGER_H

#include "db/version.h"
#include "db/version_edit.h"

#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace kvs {

class ThreadPool;

namespace db {

class BaseMemTable;
class DBImpl;
class Compact;
class Config;

class VersionManager {
public:
  VersionManager(DBImpl *db, const Config *config, ThreadPool *thread_pool);

  ~VersionManager() = default;

  // No copy allowed
  VersionManager(const VersionManager &) = delete;
  VersionManager &operator=(VersionManager &) = delete;

  // Move constructor/assignment
  VersionManager(VersionManager &&) = default;
  VersionManager &operator=(VersionManager &&) = default;

  // Create latest version and apply new SSTs metadata
  void ApplyNewChanges(std::unique_ptr<VersionEdit> version_edit);

  bool NeedSSTCompaction();

  // TODO(namnh) : Remove this after implementing starting DB flow
  void CreateLatestVersion();

  // For testing
  const std::deque<std::unique_ptr<Version>> &GetVersions() const;

  const Version *GetLatestVersion() const;

  const Config *const GetConfig();

  const DBImpl *const GetDB();

private:
  // TODO(namnh) : we need a ref-count mechanism to delist version that isn't
  // referenced to anymore
  std::deque<std::unique_ptr<Version>> versions_;

  std::unique_ptr<Version> latest_version_;

  // Below are objects that VersionManager does NOT own lifetime. So, DO NOT
  // modify, including change memory that it is pointing to,
  // allocate/deallocate, etc... these objects.
  DBImpl *db_;

  const Config *config_;

  kvs::ThreadPool *thread_pool_;

  mutable std::mutex mutex_;
};

} // namespace db

} // namespace kvs

#endif // DB_VERSION_MANAGER_H