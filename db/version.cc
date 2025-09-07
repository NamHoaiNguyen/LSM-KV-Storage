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
      compact_(std::make_unique<Compact>(this)), db_(db), config_(config),
      thread_pool_(thread_pool) {}

void Version::CreateNewSSTs(
    const std::vector<std::unique_ptr<BaseMemTable>> &immutable_memtables) {

  std::latch all_done(immutable_memtables.size());

  // TODO(namnh) : Do we need to acquire lock ?
  for (const auto &immutable_memtable : immutable_memtables) {
    thread_pool_->Enqueue(&Version::CreateNewSST, this,
                          std::cref(immutable_memtable), std::ref(all_done));
  }

  // Wait until all workers have finished
  all_done.wait();

  // if (levels_sst_info_[0].size() >= config_->GetLvl0SSTCompactionTrigger()) {
  //   thread_pool_->Enqueue(&Compact::PickCompact, compact_.get(),
  //                         levels_sst_info_[0].size());
  // }
}

void Version::CreateNewSST(
    const std::unique_ptr<BaseMemTable> &immutable_memtable,
    std::latch &work_done) {
  std::string next_sst = std::to_string(db_->GetNextSSTId());
  std::string filename = config_->GetSavedDataPath() + next_sst + ".sst";
  auto new_sst = std::make_unique<sstable::Table>(std::move(filename), config_);
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
  }

  // Signal that this worker is done
  work_done.count_down();
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