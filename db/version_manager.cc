#include "db/version_manager.h"

#include "common/base_iterator.h"
#include "common/thread_pool.h"
#include "db/base_memtable.h"
#include "db/compact.h"
#include "db/config.h"
#include "db/version.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "sstable/block_builder.h"
#include "sstable/block_index.h"
#include "sstable/table.h"

// libC++
#include <cassert>

namespace kvs {

namespace db {

VersionManager::VersionManager(DBImpl *db, const Config *config,
                               ThreadPool *thread_pool)
    : db_(db), config_(config), thread_pool_(thread_pool) {}

void VersionManager::CreateLatestVersion() {
  if (!latest_version_) {
    latest_version_ = std::make_unique<Version>(db_, config_, thread_pool_);
  }
}

void VersionManager::ApplyNewChanges(
    std::unique_ptr<VersionEdit> version_edit) {
  assert(version_edit);
  assert(latest_version_);

  auto latest_tmp_version =
      std::make_unique<Version>(db_, config_, thread_pool_);

  // Get info of SST from previous version
  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>>
      &old_version_sst_info = latest_version_->GetImmutableSSTMetadata();

  std::vector<std::vector<std::shared_ptr<SSTMetadata>>>
      &latest_version_sst_info = latest_tmp_version->GetSSTMetadata();

  // Get info of deleted files
  const std::set<std::pair<SSTId, int>> &deleted_files =
      version_edit->GetImmutableDeletedFiles();

  // Apply all ssts info of previous version
  for (int level = 0; level < config_->GetSSTNumLvels(); level++) {
    for (const auto &sst_info : old_version_sst_info[level]) {
      // If file are in list of should be deleted file, skip
      if (deleted_files.find({sst_info->table_->GetTableId(),
                              sst_info->level}) != deleted_files.end()) {
        continue;
      }

      latest_version_sst_info[level].push_back(sst_info);
    }
  }

  // Apply new SST files that are created for new verison
  const std::vector<std::shared_ptr<SSTMetadata>> &added_files =
      version_edit->GetImmutableNewFiles();
  for (const auto &file : added_files) {
    latest_version_sst_info[file->level].push_back(file);
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