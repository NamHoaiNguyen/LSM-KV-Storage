#include "db/version_manager.h"

#include "common/thread_pool.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "db/version.h"
#include "sstable/block_reader_cache.h"
#include "sstable/table_reader_cache.h"

// libC++
#include <algorithm>
#include <cassert>
#include <filesystem>

namespace fs = std::filesystem;

namespace kvs {

namespace db {

VersionManager::VersionManager(const DBImpl *db,
                               const kvs::ThreadPool *thread_pool)
    : db_(db), table_reader_cache_(db_->GetTableReaderCache()),
      block_reader_cache_(db_->GetBlockReaderCache()),
      config_(db_->GetConfig()), thread_pool_(thread_pool) {
  assert(db_ && table_reader_cache_ && block_reader_cache_ && config_ &&
         thread_pool_);
}

void VersionManager::RemoveObsoleteVersion(uint64_t version_id) {
  std::scoped_lock lock(mutex_);
  auto it = versions_.find(version_id);
  if (it == versions_.end()) {
    return;
  }

  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &sst_metadata =
      it->second->GetSSTMetadata();
  for (int level = 0; level < sst_metadata.size(); level++) {
    for (int file_index = 0; file_index < sst_metadata[level].size();
         file_index++) {
      // Decrease number of version that is refering to a file when an obsolete
      // version is deleted
      // sst_metadata[level][file_index]->ref_count--;
      // if (sst_metadata[level][file_index]->ref_count == 0) {
      if (sst_metadata[level][file_index]->ref_count.fetch_sub(1) == 1) {
        // If ref count of a SST file = 0, it means that versions refer to it no
        // more. Time to say goodbye!
        fs::path file_path(sst_metadata[level][file_index]->filename);
        if (fs::exists(file_path) && fs::is_regular_file(file_path)) {
          fs::remove(file_path);
        }
      }
    }
  }

  versions_.erase(it);
}

void VersionManager::ApplyNewChanges(
    std::unique_ptr<VersionEdit> version_edit) {
  std::scoped_lock lock(mutex_);

  assert(version_edit);

  if (!latest_version_) {
    InitVersionWhenLoadingDb(std::move(version_edit));
    return;
  }

  CreateNewVersion(std::move(version_edit));
}

void VersionManager::InitVersionWhenLoadingDb(
    std::unique_ptr<VersionEdit> version_edit) {
  next_version_id_.fetch_add(1);
  uint64_t new_verions_id = next_version_id_;
  auto new_version = std::make_unique<Version>(
      new_verions_id, config_->GetSSTNumLvels(), thread_pool_, this);

  std::vector<std::vector<std::shared_ptr<SSTMetadata>>>
      &latest_version_sst_info = new_version->GetSSTMetadata();
  std::vector<double> &latest_levels_score = new_version->GetLevelsScore();

  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &add_files =
      version_edit->GetImmutableNewFiles();

  for (int level = 0; level < add_files.size(); level++) {
    for (const auto &sst_info : add_files[level]) {
      // Increase refcount of SST metadata each time a new version is
      // created
      // sst_info->ref_count++;
      sst_info->ref_count.fetch_add(1);

      latest_version_sst_info[level].push_back(sst_info);
    }

    // Calcuate score ranking of each level
    latest_levels_score[level] =
        static_cast<double>(latest_version_sst_info[0].size()) /
        static_cast<double>(config_->GetLvl0SSTCompactionTrigger());

    if (level >= 1) {
      // All SST files level >=1 must be sorted base on smallest key.
      // Note : Because files levels >= 1 don't overlap with each other, so use
      // smallest_key for condition to sort is enough
      std::sort(latest_version_sst_info[level].begin(),
                latest_version_sst_info[level].end(),
                [](const auto &a, const auto &b) {
                  return a->smallest_key < b->smallest_key;
                });
    }
  }

  latest_version_ = std::move(new_version);
  // Each new version created has its refcount = 1
  latest_version_->IncreaseRefCount();

  // Remove obsolete SST files
  thread_pool_->Enqueue([version_edit_ = std::move(version_edit),
                         config_ = this->config_, db_ = this->db_]() {
    const std::set<std::pair<SSTId, int>> deleted_files =
        version_edit_->GetImmutableDeletedFiles();
    for (const auto &file : deleted_files) {
      std::string filename =
          db_->GetDBPath() + std::to_string(file.first) + ".sst";
      fs::path file_path(filename);
      if (fs::exists(file_path) && fs::is_regular_file(file_path)) {
        fs::remove(file_path);
      }
    }
  });
}

void VersionManager::CreateNewVersion(
    std::unique_ptr<VersionEdit> version_edit) {
  next_version_id_.fetch_add(1);
  uint64_t new_verions_id = next_version_id_.load();
  auto new_version = std::make_unique<Version>(
      new_verions_id, config_->GetSSTNumLvels(), thread_pool_, this);

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
      // Get score ranking from previous version(to know which level should
      // be compacted)
      latest_levels_score[level] = old_levels_score[level];

      // If file are in list of should be deleted file, skip
      if (deleted_files.find({sst_info->table_id, sst_info->level}) !=
          deleted_files.end()) {
        continue;
      }

      // Increase refcount of SST metadata each time a new version is
      // created
      // sst_info->ref_count++;
      sst_info->ref_count.fetch_add(1);

      latest_version_sst_info[level].push_back(sst_info);
    }
  }

  // Apply new SST files that are created for new verison
  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &added_files =
      version_edit->GetImmutableNewFiles();

  // Merge two sorted file
  for (int level = 0; level < config_->GetSSTNumLvels(); level++) {
    if (added_files[level].empty()) {
      continue;
    }

    std::for_each(added_files[level].begin(), added_files[level].end(),
                  [](const auto &elem) {
                    // return elem->ref_count++;
                    return elem->ref_count.fetch_add(1);
                  });

    if (level == 0) {
      latest_version_sst_info[level].insert(
          latest_version_sst_info[level].end(), added_files[level].begin(),
          added_files[level].end());
      continue;
    }

    // With level >= 1. add new file and sort based on smallest key in
    // ascending order
    const std::vector<std::shared_ptr<SSTMetadata>> &added_files_at_level =
        added_files[level];
    std::vector<std::shared_ptr<SSTMetadata>> new_sst_files;
    new_sst_files.reserve(latest_version_sst_info[level].size() +
                          added_files_at_level.size());

    // Merge 2 sorted vector
    std::merge(latest_version_sst_info[level].begin(),
               latest_version_sst_info[level].end(), added_files[level].begin(),
               added_files[level].end(), std::back_inserter(new_sst_files),
               [](const auto &a, const auto &b) {
                 return a->smallest_key < b->smallest_key;
               });
    // Assign back
    latest_version_sst_info[level] = std::move(new_sst_files);

    // // Update level's score
    // latest_levels_score[level] =
    //     static_cast<double>(latest_version_sst_info[level].size()) /
    //     static_cast<double>(config_->GetLvl0SSTCompactionTrigger());
  }

  // Update level 0' score
  latest_levels_score[0] =
      static_cast<double>(latest_version_sst_info[0].size()) /
      static_cast<double>(config_->GetLvl0SSTCompactionTrigger());

  // new version becomes latest version
  const Version *latest_verion_copy = latest_version_.get();
  bool success = versions_
                     .insert({latest_verion_copy->GetVersionId(),
                              std::move(latest_version_)})
                     .second;
  assert(success);
  // Decreate ref count of old version
  latest_verion_copy->DecreaseRefCount();
  latest_version_ = std::move(new_version);
  // Each new version created has its refcount = 1
  latest_version_->IncreaseRefCount();
}

GetStatus VersionManager::GetKey(std::string_view key, TxnId txn_id,
                                 SSTId table_id, uint64_t file_size) const {
  // No need to acquire mutex. Because this method is read-only
  return table_reader_cache_->GetKeyFromTableCache(
      key, txn_id, table_id, file_size, block_reader_cache_);
}

bool VersionManager::NeedSSTCompaction() const {
  std::scoped_lock lock(mutex_);
  if (!latest_version_) {
    return false;
  }

  return latest_version_->NeedCompaction();
}

const Version *VersionManager::GetLatestVersion() const {
  std::scoped_lock lock(mutex_);
  return latest_version_.get();
}

const std::unordered_map<uint64_t, std::unique_ptr<Version>> &
VersionManager::GetVersions() const {
  std::scoped_lock lock(mutex_);
  return versions_;
}

const Config *const VersionManager::GetConfig() { return config_; }

} // namespace db

} // namespace kvs