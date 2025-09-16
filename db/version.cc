#include "db/version.h"

#include "common/thread_pool.h"
#include "db/config.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "sstable/table_reader.h"

namespace kvs {

namespace db {

Version::Version(uint64_t version_id, const Config *config,
                 kvs::ThreadPool *thread_pool, VersionManager *version_manager)
    : version_id_(version_id), levels_sst_info_(config->GetSSTNumLvels()),
      // compact_(std::make_unique<Compact>(this)),
      compaction_level_(0), compaction_score_(0),
      levels_score_(config->GetSSTNumLvels(), 0), config_(config),
      thread_pool_(thread_pool), version_manager_(version_manager) {
  assert(config_ && thread_pool_ && version_manager_);
}

void Version::IncreaseRefCount() const {
  std::scoped_lock lock(ref_count_mutex_);
  ref_count_++;
}

void Version::DecreaseRefCount() const {
  std::scoped_lock lock(ref_count_mutex_);
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
  for (const auto &sst : std::views::reverse(levels_sst_info_[0])) {
    if (key < sst->smallest_key || key > sst->largest_key) {
      continue;
    }

    // TODO(namnh) : Implement bloom filter
    status = sst->table_->SearchKey(key, txn_id);
    if (status.type != db::ValueType::NOT_FOUND) {
      break;
    }
  }

  return status;
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
Version::GetSstMetadata() const {
  return levels_sst_info_;
}

} // namespace db

} // namespace kvs