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

void Version::CreateNewSSTs(
    const std::vector<std::unique_ptr<BaseMemTable>> &immutable_memtables) {

  std::latch all_done(immutable_memtables.size());

  // TODO(namnh) : Do we need to acquire lock ?
  for (const auto &immutable_memtable : immutable_memtables) {
    thread_pool_->Enqueue(&Version::CreateNewSST, this,
                          std::cref(immutable_memtable), db_->GetNextSSTId(),
                          std::ref(all_done));
  }

  // Wait until all workers have finished
  all_done.wait();
}

void Version::CreateNewSST(
    const std::unique_ptr<BaseMemTable> &immutable_memtable, uint64_t sst_id,
    std::latch &work_done) {
  std::string filename =
      config_->GetSavedDataPath() + std::to_string(sst_id) + ".sst";
  auto new_sst =
      std::make_shared<sstable::Table>(std::move(filename), sst_id, config_);
  if (!new_sst->Open()) {
    work_done.count_down();
    return;
  }

  auto iterator = std::make_unique<MemTableIterator>(immutable_memtable.get());
  // Iterate through all key/value pairs to add them to sst
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    new_sst->AddEntry(iterator->GetKey(), iterator->GetValue(),
                      iterator->GetTransactionId(), iterator->GetType());
  }

  // All of key/value pairs had been written to sst. SST should be flushed to
  // disk
  new_sst->Finish();

  {
    std::scoped_lock lock(mutex_);
    // Update new sst(at level 0) info into this version
    levels_sst_info_[0].emplace_back(
        std::make_shared<Version::SSTInfo>(std::move(new_sst)));
    // Update level 0' score
    levels_score_[0] =
        static_cast<double>(levels_sst_info_[0].size()) /
        static_cast<double>(config_->GetLvl0SSTCompactionTrigger());
  }

  // Signal that this worker is done
  work_done.count_down();
}

bool Version::NeedCompaction() {
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

size_t Version::GetNumberSSTLvl0Files() { return levels_sst_info_[0].size(); }

// SST INFO
Version::SSTInfo::SSTInfo() : should_be_deleted_(false) {}

Version::SSTInfo::SSTInfo(std::shared_ptr<sstable::Table> table)
    : should_be_deleted_(false), table_(std::move(table)) {}

} // namespace db

} // namespace kvs