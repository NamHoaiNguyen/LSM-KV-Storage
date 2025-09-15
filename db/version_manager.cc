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
#include <algorithm>
#include <cassert>

namespace kvs {

namespace db {

VersionManager::VersionManager(DBImpl *db, const Config *config,
                               kvs::ThreadPool *thread_pool)
    : db_(db), config_(config), thread_pool_(thread_pool) {}

void VersionManager::RemoveObsoleteVersion(uint64_t version_id) {
  std::scoped_lock lock(mutex_);
  versions_.erase(std::remove_if(versions_.begin(), versions_.end(),
                                 [version_id](const auto &version) {
                                   return version->GetVersionId() == version_id;
                                 }),
                  versions_.end());
}

void VersionManager::CreateLatestVersion() {
  if (!latest_version_) {
    latest_version_ = std::make_unique<Version>(++next_version_id_, config_,
                                                thread_pool_, this);
    latest_version_->IncreaseRefCount();
  }
}

void VersionManager::ApplyNewChanges(
    std::unique_ptr<VersionEdit> version_edit) {
  std::scoped_lock lock(mutex_);

  assert(version_edit);
  assert(latest_version_);

  auto new_version = std::make_unique<Version>(++next_version_id_, config_,
                                               thread_pool_, this);

  // Get info of SST from previous version
  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>>
      &old_version_sst_info = latest_version_->GetImmutableSSTMetadata();
  const std::vector<double> &old_levels_score =
      latest_version_->GetLevelsScore();

  // Prepare for latest version
  std::vector<std::vector<std::shared_ptr<SSTMetadata>>>
      &latest_version_sst_info = new_version->GetSSTMetadata();
  std::vector<double> &latest_levels_score = new_version->GetLevelsScore();

  // Get info of deleted files
  const std::set<std::pair<SSTId, int>> &deleted_files =
      version_edit->GetImmutableDeletedFiles();

  // Apply all ssts info of previous version
  for (int level = 0; level < config_->GetSSTNumLvels(); level++) {
    for (const auto &sst_info : old_version_sst_info[level]) {
      // Get score ranking from previous version(to know which level should be
      // compacted)
      latest_levels_score[level] = old_levels_score[level];

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

  // Update level 0' score
  latest_levels_score[0] =
      static_cast<double>(latest_version_sst_info[0].size()) /
      static_cast<double>(config_->GetLvl0SSTCompactionTrigger());

  // new version becomes latest version
  versions_.push_front(std::move(latest_version_));
  // Decreate ref count of old version
  assert(versions_.front()->GetRefCount() > 0);
  versions_.front()->DecreaseRefCount();
  latest_version_ = std::move(new_version);
  // Each new version created has its refcount = 1
  latest_version_->IncreaseRefCount();
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

Version *VersionManager::GetLatestVersion() const {
  std::scoped_lock lock(mutex_);
  return latest_version_.get();
}

const Config *const VersionManager::GetConfig() { return config_; }

const DBImpl *const VersionManager::GetDB() { return db_; }

} // namespace db

} // namespace kvs