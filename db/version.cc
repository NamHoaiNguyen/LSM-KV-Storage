#include "db/version.h"

#include "common/thread_pool.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "sstable/table_reader.h"

#include <algorithm>
#include <iostream>

namespace kvs {

namespace db {

Version::Version(uint64_t version_id, int num_sst_levels,
                 const kvs::ThreadPool *thread_pool,
                 VersionManager *version_manager)
    : version_id_(version_id), levels_sst_info_(num_sst_levels),
      compaction_level_(0), compaction_score_(0), ref_count_(0),
      levels_score_(num_sst_levels, 0), thread_pool_(thread_pool),
      version_manager_(version_manager) {
  assert(thread_pool_ && version_manager_);
}

void Version::IncreaseRefCount() const { ref_count_.fetch_add(1); }

void Version::DecreaseRefCount() const {
  assert(ref_count_ >= 1);
  if (ref_count_.fetch_sub(1) == 1) {
    thread_pool_->Enqueue(&VersionManager::RemoveObsoleteVersion,
                          version_manager_, version_id_);
  }
}

GetStatus Version::Get(std::string_view key, TxnId txn_id) const {
  GetStatus status;
  std::vector<std::shared_ptr<SSTMetadata>> sst_lvl0_candidates_;

  for (const auto &sst : levels_sst_info_[0]) {
    // With SSTs lvl0, because of overlapping, we need to lookup in all SSTs
    // that maybe contain the key
    if (key < sst->smallest_key || key > sst->largest_key) {
      continue;
    }

    // TODO(namnh) : Implement bloom filter for level = 0
    sst_lvl0_candidates_.push_back(sst);
  }

  // Sort in descending order based on table_id. Because we need to search from
  // newset to oldest
  std::sort(
      sst_lvl0_candidates_.begin(), sst_lvl0_candidates_.end(),
      [](const auto &a, const auto &b) { return a->table_id > b->table_id; });

  for (const auto &candidate : sst_lvl0_candidates_) {
    status = version_manager_->GetKey(key, txn_id, candidate->table_id,
                                      candidate->file_size);

    if (status.type == db::ValueType::PUT ||
        status.type == db::ValueType::DELETED ||
        status.type == db::ValueType::kTooManyOpenFiles) {
      return status;
    }
  }

  // If key is not found in SSTs lvl0, continue lookup in deeper levels
  for (int level = 1; level < levels_sst_info_.size(); level++) {
    // With level >= 1, because overlapping key doesn't happen and each file is
    // sorted based on smallest key(and largest key), we can use binary search
    // to quickly determine the file candidate to lookup
    std::shared_ptr<SSTMetadata> file_candidate = FindFilesAtLevel(level, key);
    if (!file_candidate) {
      continue;
    }

    // TODO(namnh) : Implement bloom filter for level >= 1
    status = version_manager_->GetKey(key, txn_id, file_candidate->table_id,
                                      file_candidate->file_size);
    if (status.type == db::ValueType::NOT_FOUND) {
      std::cout << key << " can't be found at level 1 " << std::endl;
    }

    if (status.type == db::ValueType::PUT ||
        status.type == db::ValueType::DELETED ||
        status.type == db::ValueType::kTooManyOpenFiles) {
      return status;
    }
  }

  // std::cout << "namnh check that we can't found the key " << key <<
  // std::endl;

  return status;
}

std::shared_ptr<SSTMetadata>
Version::FindFilesAtLevel(int level, std::string_view key) const {
  int64_t left = 0;
  int64_t right = levels_sst_info_[level].size() - 1;

  while (left < right) {
    int64_t mid = left + (right - left) / 2;
    if (levels_sst_info_[level][mid]->largest_key >= key) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }

  if (right < 0) {
    return nullptr;
  }

  return levels_sst_info_[level][right]->smallest_key <= key &&
                 key <= levels_sst_info_[level][right]->largest_key
             ? levels_sst_info_[level][right]
             : nullptr;
}

bool Version::NeedCompaction() const {
  double score_compact = 1;
  for (int level = 0; level < levels_score_.size(); level++) {
    if (levels_score_[level] >= score_compact) {
      return true;
    }
  }

  return false;
}

std::optional<int> Version::GetLevelToCompact() const {
  double highest_level_score = 0;
  int level_to_compact;

  for (int level = 0; level < levels_score_.size(); level++) {
    if (levels_score_[level] >= highest_level_score) {
      highest_level_score = levels_score_[level];
      level_to_compact = level;
    }
  }

  return level_to_compact;
}

const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &
Version::GetImmutableSSTMetadata() const {
  return levels_sst_info_;
}

// This methos is ONLY called when building data for new version
std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &
Version::GetSSTMetadata() {
  return levels_sst_info_;
}

const std::vector<double> &Version::GetImmutableLevelsScore() const {
  return levels_score_;
}

// This methos is ONLY called when building data for new version
std::vector<double> &Version::GetLevelsScore() { return levels_score_; }

size_t Version::GetNumberSSTFilesAtLevel(int level) const {
  assert(level < levels_sst_info_.size());
  return levels_sst_info_[level].size();
}

uint64_t Version::GetVersionId() const { return version_id_; }

uint64_t Version::GetRefCount() const { return ref_count_.load(); }

// For testing
const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &
Version::GetSSTMetadata() const {
  return levels_sst_info_;
}

} // namespace db

} // namespace kvs