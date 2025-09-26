#include "db/version.h"

#include "common/thread_pool.h"
#include "db/config.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "sstable/table_reader.h"

#include <iostream>

namespace kvs {

namespace db {

Version::Version(uint64_t version_id, const Config *config,
                 kvs::ThreadPool *thread_pool, VersionManager *version_manager)
    : version_id_(version_id), levels_sst_info_(config->GetSSTNumLvels()),
      // compact_(std::make_unique<Compact>(this)),
      compaction_level_(0), compaction_score_(0), ref_count_(0),
      levels_score_(config->GetSSTNumLvels(), 0), config_(config),
      thread_pool_(thread_pool), version_manager_(version_manager) {
  assert(config_ && thread_pool_ && version_manager_);
}

// Version::Version(Version &&other) {
//   version_id_ = other.version_id_;
//   levels_sst_info_ = std::move(other.levels_sst_info_);
//   compaction_level_ = other.compaction_level_;
//   compaction_score_ = other.compaction_score_;
//   levels_score_ = std::move(levels_score_);
//   ref_count_.store(other.ref_count_.load());
//   config_ = other.config_;
//   thread_pool_ = other.thread_pool_;
//   version_manager_ = other.version_manager_;
// }

// Version &Version::operator=(Version &&other) {
//   version_id_ = other.version_id_;
//   levels_sst_info_ = std::move(other.levels_sst_info_);
//   compaction_level_ = other.compaction_level_;
//   compaction_score_ = other.compaction_score_;
//   levels_score_ = std::move(levels_score_);
//   ref_count_.store(other.ref_count_.load());
//   config_ = other.config_;
//   thread_pool_ = other.thread_pool_;
//   version_manager_ = other.version_manager_;

//   return *this;
// }

void Version::IncreaseRefCount() const {
  std::scoped_lock lock(ref_count_mutex_);
  ref_count_++;
}

void Version::DecreaseRefCount() const {
  std::scoped_lock lock(ref_count_mutex_);
  assert(ref_count_ >= 1);
  ref_count_--;
  if (ref_count_ == 0) {
    thread_pool_->Enqueue(&VersionManager::RemoveObsoleteVersion,
                          version_manager_, version_id_);
  }
}

GetStatus Version::Get(std::string_view key, TxnId txn_id) const {
  GetStatus status;

  // Search in SSTs lvl0
  // Files are saved from oldest-to-newest
  if (levels_sst_info_.size() != config_->GetSSTNumLvels()) {
    std::cout << "namnh check size of levels_sst_info_" << std::endl;
  }

  for (const auto &sst : std::views::reverse(levels_sst_info_[0])) {
    if (key < sst->smallest_key || key > sst->largest_key) {
      continue;
    }

    // TODO(namnh) : Implement bloom filter

    status =
        version_manager_->GetKey(key, txn_id, sst->table_id, sst->file_size);
    if (status.type != db::ValueType::NOT_FOUND) {
      // break;
      return status;
    }
  }

  if (levels_sst_info_.size() != config_->GetSSTNumLvels()) {
    std::cout << "namnh check size of levels_sst_info_" << std::endl;
  }

  // Continue search in deeper level
  for (int level = 1; level < levels_sst_info_.size(); level++) {
    for (const auto &sst : levels_sst_info_[level]) {
      // if (key < sst->smallest_key || key > sst->largest_key) {
      //   continue;
      // }

      status =
          version_manager_->GetKey(key, txn_id, sst->table_id, sst->file_size);
      if (status.type != db::ValueType::NOT_FOUND) {
        break;
      }
    }
  }

  return status;
}

int Version::FindFilesAtLevel(int level, std::string_view key) {
  size_t left = 0;
  size_t right = levels_sst_info_[level].size() - 1;

  while (left < right) {
    size_t mid = left + (right - left) / 2;
    if (levels_sst_info_[level][mid]->largest_key >= key) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }

  return right;
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

uint64_t Version::GetRefCount() const {
  std::scoped_lock lock(ref_count_mutex_);
  return ref_count_;
}

// For testing
const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &
Version::GetSSTMetadata() const {
  return levels_sst_info_;
}

} // namespace db

} // namespace kvs