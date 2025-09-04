#include "db/version.h"

#include "common/thread_pool.h"
#include "db/compact.h"
#include "db/config.h"
#include "db/db_impl.h"
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
      compact_(std::make_unique<Compact>(this)), db_(db), config_(config),
      thread_pool_(thread_pool) {}

bool Version::CreateNewSST(const BaseMemTable *const immutable_memtable) {
  std::string next_sst = std::to_string(db_->GetNextSSTId());
  std::string filename = config_->GetSavedDataPath() + next_sst + ".sst";
  auto new_sst = std::make_unique<sstable::Table>(std::move(filename), config_);
  if (!new_sst->Open()) {
    return false;
  }

  // TODO(namnh) : Do we need to acquire lock ?
  auto iterator = std::make_unique<MemTableIterator>(immutable_memtable);

  // Iterate through all key/value pairs to add them to sst
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    new_sst->AddEntry(iterator->GetKey(), iterator->GetValue(),
                      iterator->GetTransactionId(), iterator->GetType());
  }

  // All of key/value pairs had been written to sst. SST should be flushed to
  // disk
  new_sst->Finish();

  // Update new sst(at level 0) info into this version
  levels_sst_info_[0].emplace_back(
      std::make_unique<Version::SSTInfo>(std::move(new_sst)));

  if (levels_sst_info_[0].size() >= config_->GetLvl0SSTCompactionTrigger()) {
    thread_pool_->Enqueue(&Compact::PickCompact, compact_.get(),
                          levels_sst_info_[0].size());
  }

  return true;
}

const std::vector<std::vector<std::shared_ptr<Version::SSTInfo>>> &
Version::GetImmutableSSTInfo() const {
  return levels_sst_info_;
}

std::vector<std::vector<std::shared_ptr<Version::SSTInfo>>> &
Version::GetSSTInfo() {
  return levels_sst_info_;
}

// SST INFO
Version::SSTInfo::SSTInfo() : should_be_deleted_(false) {}

Version::SSTInfo::SSTInfo(std::unique_ptr<sstable::Table> table)
    : should_be_deleted_(false), table_(std::move(table)) {}

} // namespace db

} // namespace kvs