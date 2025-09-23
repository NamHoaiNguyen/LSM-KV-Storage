#include "db/compact.h"

#include "common/base_iterator.h"
#include "common/macros.h"
#include "db/db_impl.h"
#include "db/merge_iterator.h"
#include "db/version.h"
#include "sstable/block_reader_iterator.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"
#include "sstable/table_reader_iterator.h"

// libC++
#include <cassert>
#include <iostream>

namespace kvs {

namespace db {

Compact::Compact(const sstable::BlockReaderCache *block_reader_cache,
                 const sstable::TableReaderCache *table_reader_cache,
                 const Version *version, DBImpl* db) {}
    : block_reader_cache_(block_reader_cache),
      table_reader_cache_(table_reader_cache), version_(version),
      version_edit_(version_edit), db_(db) {
  assert(block_reader_cache_ && table_reader_cache_ && version_ && db_);
}

void Compact::PickCompact() {
  // TODO(namnh) : Do we need to acquire lock ?
  // What happen if when compact is executing, new SST file appears ?
  if (!version_) {
    return;
  }

  // if (version_->levels_sst_info_[0].empty()) {
  //   // TODO(namnh) : check that higher level need to merge or not ?
  //   // DoFullCompact();
  //   return;
  // }

  if (!version_->GetLevelToCompact()) {
    return;
  }

  int level_to_compact = version_->GetLevelToCompact().value();
  if (level_to_compact == 0) {
    DoL0L1Compact();
    return;
  }

  // DoOtherLevelsCompact();
}

void Compact::DoL0L1Compact() {
  // Get oldest sst level 0(the first lvl 0 file. Because sst files are sorted)
  // TODO(namnh) : Recheck this logic
  // Get overlapping lvl0 sst files
  std::pair<std::string_view, std::string_view> keys = GetOverlappingSSTLvl0();

  // Get overlapping lvl1 sst files
  GetOverlappingSSTOtherLvls(1 /*level*/, keys.first /*smallest_key*/,
                             keys.second /*largest_key*/);
  // Execute compaction
  DoCompactJob();
}

std::pair<std::string_view, std::string_view> Compact::GetOverlappingSSTLvl0() {
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

  // Add oldest lvl 0 to compact list
  files_need_compaction_[0].push_back(
      version_->levels_sst_info_[0][oldest_sst_index].get());

  // Iterate through all sst lvl 0
  for (int i = 0; i < version_->levels_sst_info_[0].size(); i++) {
    if (smallest_key <= version_->levels_sst_info_[0][i]->smallest_key &&
        largest_key >= version_->levels_sst_info_[0][i]->largest_key) {
      // If overllaping, this file should also be added to compact list
      files_need_compaction_[0].push_back(
          version_->levels_sst_info_[0][i].get());

      if (version_->levels_sst_info_[0][i]->smallest_key < smallest_key) {
        // Update smallest key
        smallest_key = version_->levels_sst_info_[0][i]->smallest_key;
        // Re-iterating
        i = 0;
      }

      if (largest_key < version_->levels_sst_info_[0][i]->largest_key) {
        // Update largest key
        largest_key = version_->levels_sst_info_[0][i]->largest_key;
        // Re-iterating
        i = 0;
      }
    }
  }

  return {smallest_key, largest_key};
}

void Compact::GetOverlappingSSTOtherLvls(int level,
                                         std::string_view smallest_key,
                                         std::string_view largest_key) {
  std::optional<size_t> starting_file_index =
      FindNonOverlappingFiles(level, smallest_key, largest_key);

  if (!starting_file_index) {
    return;
  }

  for (size_t i = starting_file_index.value();
       i < version_->levels_sst_info_[level].size(); i++) {
    if (smallest_key <= version_->levels_sst_info_[level][i]->largest_key &&
        largest_key >= version_->levels_sst_info_[level][i]->smallest_key) {
      files_need_compaction_[1].push_back(
          version_->levels_sst_info_[level][i].get());
    } else {
      // Because at these levels, files are not overllaping with each other.
      // Because 'i.smallest_key' > 'i - 1.largest_key' then if file at "i"
      // index doesn't overllap, then all files after it doesn't.
      break;
    }
  }
}

std::optional<size_t>
Compact::FindNonOverlappingFiles(int level, std::string_view smallest_key,
                                 std::string_view largest_key) {
  // We don't need to lock. Because all of sst files are
  // immutable.
  // TODO(namnh) : Recheck above

  assert(level >= 1);
  if (version_->levels_sst_info_[level].empty()) {
    return std::nullopt;
  }

  size_t left = 0;
  // TODO(namnh) : SAFE ?
  size_t right = version_->levels_sst_info_[level].size() - 1;

  while (left < right) {
    size_t mid = left + (right - left) / 2;
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

std::unique_ptr<kvs::BaseIterator> Compact::CreateMergeIterator() {
  std::vector<std::unique_ptr<sstable::TableReaderIterator>>
      table_reader_iterators;
  std::string filename;
  SSTId table_id;

  for (int level = 0; level < 2; level++) {
    for (int i = 0; i < files_need_compaction_[level].size(); i++) {
      filename = files_need_compaction_[level][i]->filename;
      table_id = files_need_compaction_[level][i]->table_id;
      // Find table in cache
      const sstable::TableReader *table_reader =
          table_reader_cache_->GetTableReader(table_id);
      if (!table_reader) {
        // If not found, create new table and add into cache
        std::string filename = files_need_compaction_[level][i]->filename;
        uint64_t file_size = files_need_compaction_[level][i]->file_size;
        auto new_table_reader = sstable::CreateAndSetupDataForTableReader(
            std::move(filename), table_id, file_size);
        // create iterator for new table
        table_reader_iterators.emplace_back(
            std::make_unique<sstable::TableReaderIterator>(
                block_reader_cache_, new_table_reader.get()));

        // Insert new table into cache
        table_reader_cache_->AddNewTableReader(table_id,
                                               std::move(new_table_reader));
        continue;
      }

      // If table reader had already been in cache, just create table iterator
      table_reader_iterators.emplace_back(
          std::make_unique<sstable::TableReaderIterator>(block_reader_cache_,
                                                         table_reader));
    }
  }

  return std::make_unique<MergeIterator>(std::move(table_reader_iterators));
}

void Compact::DoCompactJob() {
  std::vector<std::unique_ptr<sstable::TableReaderIterator>>
      table_reader_iterators;
  std::string_view last_current_key = std::string_view{};
  auto version_edit = std::make_unique<VersionEdit>();

  uint64_t new_sst_id = db_->GetNextSSTId();
  std::string filename =
      db_->GetConfig()->GetSavedDataPath() +
          std::to_string(new_sst_id) + ".sst";
   
  auto new_sst = std::make_unique<sstable::TableBuilder>
                    (std::move(filename), config_.get());

  if (!new_sst.Open()) {
    return;
  }

  std::unique_ptr<kvs::BaseIterator> iterator = CreateMergeIterator();
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    std::string_view key = iterator->GetKey();
    std::string_view value = iterator->GetValue();
    db::ValueType type = iterator->GetType();
    TxnId txn_id = iterator->GetTransactionId();
    assert(type == db::ValueType::PUT || type == db::ValueType::DELETED);
    
    // Filter
    if (!ShouldPickEntry()) {
      continue;
    }

    new_sst->AddEntry(key, value, txn_id, type);
    if (new_sst->GetFileSize() >= db_->GetConfig()->GetSSTBlockSize()) {
      new_sst->Finish();
      version_edit->AddNewFiles(new_sst_id, 0 /*level*/, new_sst.GetFileSize(),
                                new_sst.GetSmallestKey(), new_sst.GetLargestKey(),
                                std::move(filename));
      if (iterator->IsValid()) {
        // If still have data, it means that a new SST will be created
        new_sst_id = db_->GetNextSSTId();
        filename = db_->GetConfig()->GetSavedDataPath()
                      + std::to_string(new_sst_id) + ".sst";
        new_sst.reset(new sstable::TableBuilder(std::move(filename), config_.get()));
        if (!new_sst.Open()) {
          return;
        }
      }
    }
  }

  // All files that need to be compacted should be deleted after all
  for (int level = 0; level < 2; level++) {
    for (int i = 0; i < files_need_compaction_[level].size(); i++) {
      version_edit->RemoveFiles(files_need_compaction_[level][i]->level
                                files_need_compaction_[level][i]->table_id);
    }
  }
}

bool Compact::ShouldPickEntry() {

}


} // namespace db

} // namespace kvs