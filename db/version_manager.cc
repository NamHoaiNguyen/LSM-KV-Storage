#include "db/version_manager.h"

#include "common/base_iterator.h"
#include "common/thread_pool.h"
#include "db/base_memtable.h"
#include "db/compact.h"
#include "db/config.h"
#include "db/version.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "sstable/block.h"
#include "sstable/block_index.h"
#include "sstable/sst.h"

namespace kvs {

namespace db {

VersionManager::VersionManager(DBImpl *db, const Config *config,
                               ThreadPool *thread_pool)
    : db_(db), config_(config), thread_pool_(thread_pool) {}

Version *VersionManager::CreateLatestVersion() {
  std::scoped_lock lock(mutex_);

  // First time loading db
  if (!latest_version_) {
    latest_version_ = std::make_unique<Version>(db_, config_, thread_pool_);
    return latest_version_.get();
  }

  std::unique_ptr<Version> tmp_version = std::move(latest_version_);
  latest_version_ = std::make_unique<Version>(db_, config_, thread_pool_);

  // Get info of SST from previous version
  const std::vector<std::vector<std::shared_ptr<Version::SSTInfo>>>
      &old_version_sst_info = tmp_version->GetImmutableSSTInfo();
  const std::vector<double> &old_levels_score = tmp_version->GetLevelsScore();

  std::vector<std::vector<std::shared_ptr<Version::SSTInfo>>>
      &latest_version_sst_info = latest_version_->GetSSTInfo();
  std::vector<double> &latest_levels_score = latest_version_->GetLevelsScore();

  for (int level = 0; level < config_->GetSSTNumLvels(); level++) {
    // Get score ranking from previous version(to know which level should be
    // compacted)
    latest_levels_score[level] = old_levels_score[level];

    for (const auto &sst_info : old_version_sst_info[level]) {
      if (sst_info->should_be_deleted_) {
        continue;
      }

      latest_version_sst_info[level].push_back(sst_info);
    }
  }

  // Add tmp_version(which is the previous version of the latest one) to dequeue
  versions_.push_front(std::move(tmp_version));

  return latest_version_.get();
}

void VersionManager::ApplyNewChanges(
    std::vector<std::shared_ptr<Version::SSTInfo>> &&new_ssts_info) {
  auto latest_tmp_version =
      std::make_unique<Version>(db_, config_, thread_pool_);

  // Get info of SST from previous version
  const std::vector<std::vector<std::shared_ptr<Version::SSTInfo>>>
      &old_version_sst_info = latest_version_->GetImmutableSSTInfo();

  std::vector<std::vector<std::shared_ptr<Version::SSTInfo>>>
      &latest_version_sst_info = latest_tmp_version->GetSSTInfo();

  // Apply all ssts info of previous version
  for (int level = 0; level < config_->GetSSTNumLvels(); level++) {
    for (const auto &sst_info : old_version_sst_info[level]) {
      if (sst_info->should_be_deleted_) {
        continue;
      }

      latest_version_sst_info[level].push_back(sst_info);
    }
  }

  // Add new SSTs info to latest version
  for (const auto &new_sst_info : new_ssts_info) {
    int level = new_sst_info->level_;
    latest_version_sst_info[level].push_back(std::move(new_sst_info));
  }

  // Create new latest version
  {
    std::scoped_lock lock(mutex_);
    versions_.push_front(std::move(latest_version_));
    latest_version_ = std::move(latest_tmp_version);
  }
}

bool VersionManager::NeedSSTCompaction() {
  std::scoped_lock lock(mutex_);
  if (!latest_version_) {
    return false;
  }

  return latest_version_->NeedCompaction();
}

const std::deque<std::unique_ptr<Version>> &
VersionManager::GetVersions() const {
  std::scoped_lock lock(mutex_);
  return versions_;
}

const Version *VersionManager::GetLatestVersion() const {
  std::scoped_lock lock(mutex_);
  return latest_version_.get();
}

const Config *const VersionManager::GetConfig() { return config_; }

const DBImpl *const VersionManager::GetDB() { return db_; }

} // namespace db

} // namespace kvs