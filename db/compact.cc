#include "db/compact.h"

#include "common/base_iterator.h"
#include "common/macros.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "db/merge_iterator.h"
#include "db/version.h"
#include "sstable/block_builder.h"
#include "sstable/block_reader_cache.h"
#include "sstable/block_reader_iterator.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"
#include "sstable/table_reader_iterator.h"

// libC++
#include <cassert>

namespace kvs {

namespace db {

Compact::Compact(const std::vector<std::unique_ptr<sstable::BlockReaderCache>>
                     &block_reader_cache,
                 const sstable::TableReaderCache *const table_reader_cache,
                 const Version *version, VersionEdit *version_edit, DBImpl *db)
    : block_reader_cache_(block_reader_cache),
      table_reader_cache_(table_reader_cache), version_(version),
      version_edit_(version_edit), db_(db) {
  assert(table_reader_cache_ && version_ && db_);
}

bool Compact::PickCompact() {
  if (!version_) {
    return false;
  }

  if (!version_->GetLevelToCompact()) {
    return false;
  }

  level_to_compact_ = version_->GetLevelToCompact().value();
  if (level_to_compact_ == 0) {
    return DoL0L1Compact();
  }

  // DoOtherLevelsCompact();

  return false;
}

bool Compact::DoL0L1Compact() {
  // Get oldest sst level 0(the first lvl 0 file. Because sst files are sorted)
  assert(!version_->levels_sst_info_[0].empty());

  // Get OLDEST lvl 0 sst(smallest number Lvl 0 sst file)
  uint64_t oldest_lvl0_sst_id = ULONG_MAX;
  int oldest_sst_index = 0;
  for (int i = 0; i < version_->levels_sst_info_[0].size(); i++) {
    if (version_->levels_sst_info_[0][i]->table_id < oldest_lvl0_sst_id) {
      oldest_lvl0_sst_id = version_->levels_sst_info_[0][i]->table_id;
      oldest_sst_index = i;
    }
  }

  std::string_view smallest_key =
      version_->levels_sst_info_[0][oldest_sst_index]->smallest_key;
  std::string_view largest_key =
      version_->levels_sst_info_[0][oldest_sst_index]->largest_key;

  // Get overlapping lvl0 sst files

  auto [smallest_key_final, largest_key_final] =
      GetOverlappingSSTLvl0(smallest_key, largest_key, oldest_sst_index);

  // Get overlapping lvl1 sst files
  GetOverlappingSSTNextLvl(1 /*level*/, smallest_key_final /*smallest_key*/,
                           largest_key_final /*largest_key*/);
  // Execute compaction
  return DoCompactJob();
}

std::pair<std::string_view, std::string_view>
Compact::GetOverlappingSSTLvl0(std::string_view smallest_key,
                               std::string_view largest_key,
                               int oldest_sst_index) {
  // Add oldest lvl 0 to compact list
  files_need_compaction_[0].push_back(
      version_->levels_sst_info_[0][oldest_sst_index].get());
  bool ShouldIterateAgain = false;

  // Iterate through all sst lvl 0
  for (int i = 0; i < version_->levels_sst_info_[0].size(); i++) {
    if (version_->levels_sst_info_[0][i]->table_id ==
        version_->levels_sst_info_[0][oldest_sst_index]->table_id) {
      continue;
    }

    if (version_->levels_sst_info_[0][i]->largest_key < smallest_key ||
        version_->levels_sst_info_[0][i]->smallest_key > largest_key) {
      continue;
    }

    // If overllaping, this file should also be added to compact list
    files_need_compaction_[0].push_back(version_->levels_sst_info_[0][i].get());

    if (version_->levels_sst_info_[0][i]->smallest_key < smallest_key) {
      // Update smallest key
      smallest_key = version_->levels_sst_info_[0][i]->smallest_key;
      // Re-iterating
      files_need_compaction_[0].clear();
      ShouldIterateAgain = true;
      break;
    }

    if (largest_key < version_->levels_sst_info_[0][i]->largest_key) {
      // Update largest key
      largest_key = version_->levels_sst_info_[0][i]->largest_key;
      // Re-iterating
      files_need_compaction_[0].clear();
      ShouldIterateAgain = true;
      break;
    }
  }

  if (ShouldIterateAgain) {
    if (smallest_key > largest_key) {
      // If re-iterating is needed, maybe there is a case that smallest key can
      // be larger than largest key. If that, they need to be swapped
      std::swap(smallest_key, largest_key);
    }

    // Because smallest key and/or smallest key is updated, need another call to
    // get all overlapping SST lvl0 files
    GetOverlappingSSTLvl0(smallest_key, largest_key, oldest_sst_index);
  }

  return {smallest_key, largest_key};
}

void Compact::GetOverlappingSSTNextLvl(int level, std::string_view smallest_key,
                                       std::string_view largest_key) {
  std::optional<size_t> starting_file_index =
      FindFile(level, smallest_key, largest_key);

  if (!starting_file_index) {
    return;
  }

  for (size_t i = starting_file_index.value();
       i < version_->levels_sst_info_[level].size(); i++) {
    files_need_compaction_[1].push_back(
        version_->levels_sst_info_[level][i].get());
  }
}

std::optional<int64_t> Compact::FindFile(int level,
                                         std::string_view smallest_key,
                                         std::string_view largest_key) {
  assert(level >= 1);
  if (version_->levels_sst_info_[level].empty()) {
    return std::nullopt;
  }

  int64_t left = 0;
  int64_t right = version_->levels_sst_info_[level].size() - 1;

  while (left < right) {
    int64_t mid = left + (right - left) / 2;
    if (version_->levels_sst_info_[level][mid]->largest_key < smallest_key) {
      // Largest key of sst index "mid" < smallest_key. Therefor all files
      // at or before "mid" are uninteresting
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target". Therefor all files after "mid"
      // are uninteresting
      right = mid;
    }
  }

  return right;
}

std::unique_ptr<MergeIterator> Compact::CreateMergeIterator() {
  std::vector<std::unique_ptr<sstable::TableReaderIterator>>
      table_reader_iterators;
  std::string filename;
  SSTId table_id;

  for (int level = 0; level < 2; level++) {
    for (int i = 0; i < files_need_compaction_[level].size(); i++) {
      // filename = files_need_compaction_[level][i]->filename;
      table_id = files_need_compaction_[level][i]->table_id;
      // Find table in cache
      std::shared_ptr<sstable::LRUTableItem> lru_table_item =
          table_reader_cache_->GetLRUTableItem(table_id);
      if (lru_table_item && lru_table_item->GetTableReader()) {
        // If table reader had already been in cache, just create table iterator
        table_reader_iterators.emplace_back(
            std::make_unique<sstable::TableReaderIterator>(block_reader_cache_,
                                                           lru_table_item));
        continue;
      }

      // If not found, create new table and add into cache
      std::string filename = files_need_compaction_[level][i]->filename;
      uint64_t file_size = files_need_compaction_[level][i]->file_size;
      auto new_table_reader = sstable::CreateAndSetupDataForTableReader(
          std::move(filename), table_id, file_size);
      if (!new_table_reader) {
        return nullptr;
      }

      auto new_lru_table_item = std::make_shared<sstable::LRUTableItem>(
          table_id, std::move(new_table_reader), table_reader_cache_);
      std::shared_ptr<sstable::LRUTableItem> table_reader_inserted =
          table_reader_cache_->AddNewTableReaderThenGet(
              table_id, new_lru_table_item, true /*add_then_get*/);

      // create iterator for new table
      table_reader_iterators.emplace_back(
          std::make_unique<sstable::TableReaderIterator>(
              block_reader_cache_, table_reader_inserted));
    }
  }

  return std::make_unique<MergeIterator>(std::move(table_reader_iterators));
}

bool Compact::DoCompactJob() {
  std::vector<std::unique_ptr<sstable::TableReaderIterator>>
      table_reader_iterators;
  uint64_t new_sst_id = db_->GetNextSSTId();
  std::string filename = db_->GetDBPath() + std::to_string(new_sst_id) + ".sst";

  auto new_sst = std::make_unique<sstable::TableBuilder>(std::move(filename),
                                                         db_->GetConfig());
  if (!new_sst->Open()) {
    db_->WakeupBgThreadToCleanupFiles(new_sst->GetFilename());
    return false;
  }

  std::unique_ptr<MergeIterator> iterator = CreateMergeIterator();
  if (!iterator) {
    return false;
  }

  std::string_view last_current_key = std::string_view{};
  TxnId last_txn_id = INVALID_TXN_ID;
  db::ValueType last_type = db::ValueType::NOT_FOUND;

  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    std::string_view key = iterator->GetKey();
    std::string_view value = iterator->GetValue();
    db::ValueType type = iterator->GetType();
    TxnId txn_id = iterator->GetTransactionId();
    assert(!key.empty() &&
           (type == db::ValueType::PUT || type == db::ValueType::DELETED) &&
           txn_id != INVALID_TXN_ID);

    // Filter
    bool should_keep_entry =
        ShouldKeepEntry(last_current_key, key, last_txn_id, txn_id, type);
    if (last_current_key != key) {
      // When a new key shows up, keep the first (newest) version.
      last_current_key = key;
      last_txn_id = txn_id;
      last_type = type;
    }

    if (!should_keep_entry) {
      continue;
    }

    if (!new_sst) {
      new_sst_id = db_->GetNextSSTId();
      filename = db_->GetDBPath() + std::to_string(new_sst_id) + ".sst";
      new_sst = std::make_unique<sstable::TableBuilder>(std::move(filename),
                                                        db_->GetConfig());

      if (!new_sst->Open()) {
        db_->WakeupBgThreadToCleanupFiles(new_sst->GetFilename());
        return false;
      }
    }

    new_sst->AddEntry(key, value, txn_id, type);
    if (new_sst->GetDataSize() >= db_->GetConfig()->GetPerMemTableSizeLimit()) {
      new_sst->Finish();
      std::string filename =
          db_->GetDBPath() + std::to_string(new_sst_id) + ".sst";
      version_edit_->AddNewFiles(new_sst_id, 1 /*level*/,
                                 new_sst->GetFileSize(),
                                 new_sst->GetSmallestKey(),
                                 new_sst->GetLargestKey(), std::move(filename));
      // TableBuilder finishes it job. Free to prepare for another TableBuilder
      // if need
      new_sst.reset();
    }
  }

  if (new_sst) {
    // Flush remaining datas
    new_sst->Finish();
    filename = db_->GetDBPath() + std::to_string(new_sst_id) + ".sst";
    version_edit_->AddNewFiles(new_sst_id, 1 /*level*/, new_sst->GetFileSize(),
                               new_sst->GetSmallestKey(),
                               new_sst->GetLargestKey(), std::move(filename));
  }

  // All files that need to be compacted should be deleted after all
  for (int level = 0; level < 2; level++) {
    for (int i = 0; i < files_need_compaction_[level].size(); i++) {
      version_edit_->RemoveFiles(files_need_compaction_[level][i]->table_id,
                                 files_need_compaction_[level][i]->level);
    }
  }

  return true;
}

bool Compact::ShouldKeepEntry(std::string_view last_current_key,
                              std::string_view key, TxnId last_txn_id,
                              TxnId txn_id, ValueType type) {
  // Logic to pick a key
  // 1. If it is the first key of mergeIterator, keep
  // 2. If there are multiple same keys, keep the one with highest Transaction
  // Id
  // 3. If key shows first time and type of key = DELETED
  // 3.1 If this key still show up at higher level, it should be kept as
  // tombstone
  // 3.2 Else we can remove this key

  if (last_current_key.empty()) {
    // First key of mergeIterator
    return true;
  }

  if (last_current_key != key) {
    // New key
    if (type == db::ValueType::PUT) {
      // just keep if type is PUT
      return true;
    } else if (type == db::ValueType::DELETED) {
      if (!IsBaseLevelForKey(key)) {
        // If this key exist at higher level, keep it as tombstone
        return true;
      } else {
        return false;
      }
    }
  }

  // Cases that meet duplicate key
  if (last_txn_id > txn_id) {
    // Only keep the one that have largest txn_id(or sequence number now)
    return false;
  }

  return true;
}

bool Compact::IsBaseLevelForKey(std::string_view key) {
  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>>
      &list_sst_metadata = version_->GetImmutableSSTMetadata();

  for (int level = level_to_compact_ + 2;
       level < db_->GetConfig()->GetSSTNumLvels(); level++) {
    for (const auto &sst_metadata : list_sst_metadata[level]) {
      if (sst_metadata->smallest_key <= key &&
          key <= sst_metadata->smallest_key) {
        return false;
      }
    }
  }

  return true;
}

} // namespace db

} // namespace kvs