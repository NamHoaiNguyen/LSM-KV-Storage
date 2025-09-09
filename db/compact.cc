#include "db/compact.h"

#include "common/macros.h"
#include "db/db_impl.h"
#include "db/version.h"
#include "sstable/sst.h"

// libC++
#include <cassert>
#include <iostream>

namespace kvs {

namespace db {

Compact::Compact(const Version *version) : version_(version) {}

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
}

std::pair<std::string_view, std::string_view> Compact::GetOverlappingSSTLvl0() {
  assert(!version_->levels_sst_info_[0].empty());

  std::string_view smallest_key =
      version_->levels_sst_info_[0][0]->table_->table_smallest_key_;
  std::string_view largest_key =
      version_->levels_sst_info_[0][0]->table_->table_largest_key_;

  // Iterate through all sst lvl 0
  for (int i = 0; i < version_->levels_sst_info_[0].size(); i++) {
    if (smallest_key <= version_->levels_sst_info_[0][i]->largest_key_ &&
        largest_key >= version_->levels_sst_info_[0][i]->smallest_key_) {
      // If overllaping, this file should also be added to compact list
      compact_info_.push_back(version_->levels_sst_info_[0][i].get());

      if (version_->levels_sst_info_[0][i]->smallest_key_ < smallest_key) {
        // Update smallest key
        smallest_key = version_->levels_sst_info_[0][i]->smallest_key_;
        // Re-iterating
        i = 0;
      }

      if (largest_key < version_->levels_sst_info_[0][i]->largest_key_) {
        // Update largest key
        largest_key = version_->levels_sst_info_[0][i]->largest_key_;
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
  if (version_->levels_sst_info_[level].empty()) {
    return;
  }

  size_t starting_file_index =
      FindNonOverlappingFiles(level, smallest_key, largest_key);

  for (size_t i = starting_file_index;
       i < version_->levels_sst_info_[level].size(); i++) {
    if (smallest_key <= version_->levels_sst_info_[level][i]->largest_key_ &&
        largest_key >= version_->levels_sst_info_[level][i]->smallest_key_) {
      compact_info_.push_back(version_->levels_sst_info_[level][i].get());
    } else {
      // Because at these levels, files are not overllaping with each other.
      // Because 'i.smallest_key' > 'i - 1.largest_key' then if file at "i"
      // index doesn't overllap, then all files after it doesn't.
      break;
    }
  }
}

size_t Compact::FindNonOverlappingFiles(uint8_t level,
                                        std::string_view smallest_key,
                                        std::string_view largest_key) {
  // We don't need to lock. Because all of sst files are
  // immutable.
  // TODO(namnh) : Recheck above

  assert(level >= 1);

  size_t left = 0;
  // TODO(namnh) : SAFE ?
  size_t right = version_->levels_sst_info_[level].size() - 1;

  while (left < right) {
    size_t mid = left + (right - left) / 2;
    if (version_->levels_sst_info_[level][mid]->largest_key_ < smallest_key) {
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

void Compact::DoCompactJob() {}

} // namespace db

} // namespace kvs