#include "db/version.h"

#include "common/thread_pool.h"
#include "db/base_memtable.h"
#include "db/compact.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/memtable_iterator.h"
#include "db/version.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "sstable/block.h"
#include "sstable/block_index.h"
#include "sstable/sst.h"

namespace kvs {

namespace db {

Version::Version(DBImpl *db, const Config *config, ThreadPool *thread_pool)
    : levels_sst_info_(config->GetSSTNumLvels()),
      // compact_(std::make_unique<Compact>(this)),
      compaction_level_(0), compaction_score_(0),
      levels_score_(config->GetSSTNumLvels(), 0), db_(db), config_(config),
      thread_pool_(thread_pool) {}

GetStatus Version::Get(std::string_view key, TxnId txn_id) const {
  GetStatus status;

  // Search in SSTs lvl0
  // for (const auto &sst : levels_sst_info_[0]) {
  for (const auto &sst : std::views::reverse(levels_sst_info_[0])) {
    if (key < sst->table_->GetSmallestKey() ||
        key > sst->table_->GetLargestKey()) {
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

void Version::ExecCompaction() {
  compact_ = std::make_unique<Compact>(this);
  compact_->PickCompact();

  // TODO(namnh) : caculate score from level 1 - n after compaction
}

const std::vector<std::vector<std::shared_ptr<Version::SSTInfo>>> &
Version::GetImmutableSSTInfo() const {
  return levels_sst_info_;
}

std::vector<std::vector<std::shared_ptr<Version::SSTInfo>>> &
Version::GetSSTInfo() {
  return levels_sst_info_;
}

const std::vector<double> &Version::GetImmutableLevelsScore() const {
  return levels_score_;
}

std::vector<double> &Version::GetLevelsScore() { return levels_score_; }

size_t Version::GetNumberSSTLvl0Files() const {
  return levels_sst_info_[0].size();
}

// SST INFO
Version::SSTInfo::SSTInfo() : should_be_deleted_(false) {}

Version::SSTInfo::SSTInfo(std::shared_ptr<sstable::Table> table, int level)
    : table_(std::move(table)), level_(level), should_be_deleted_(false) {}

} // namespace db

} // namespace kvs