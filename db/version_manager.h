#ifndef DB_VERSION_MANAGER_H
#define DB_VERSION_MANAGER_H

#include <deque>
#include <memory>

namespace kvs {

class ThreadPool;

namespace db {

class DBImpl;
class Compact;
class Config;
class Version;

class VersionManager {
public:
  VersionManager(DBImpl *db, const Config *config);

  ~VersionManager() = default;

  // No copy allowed
  VersionManager(const VersionManager &) = delete;
  VersionManager &operator=(VersionManager &) = delete;

  // Move constructor/assignment
  VersionManager(VersionManager &&) = default;
  VersionManager &operator=(VersionManager &&) = default;

  Version *CreateNewVersion();

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
};

} // namespace db

} // namespace kvs

#endif // DB_VERSION_MANAGER_H