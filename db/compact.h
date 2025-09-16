#ifndef DB_COMPACT_H
#define DB_COMPACT_H

#include "version.h"
#include "version_edit.h"

#include <cstdint>
#include <string_view>
#include <vector>

namespace kvs {

namespace db {

// KEY RULE: Compact is only triggerd by LATEST version
class Compact {
public:
  Compact(const Version *version, VersionEdit *version_edit);

  ~Compact() = default;

  // Copy constructor/assignment
  Compact(const Compact &) = delete;
  Compact &operator=(Compact &) = delete;

  // Move constructor/assignment
  Compact(Compact &&) = default;
  Compact &operator=(Compact &&) = default;

  // sst_lvl0_size works like a snapshot of SSTInfo size at the time that
  // compact is triggered
  void PickCompact();

private:
  void DoL0L1Compact();

  // Find all overlapping sst files at level
  std::pair<std::string_view, std::string_view> GetOverlappingSSTLvl0();

  void GetOverlappingSSTOtherLvls(int level, std::string_view smallest_key,
                                  std::string_view largest_key);

  // Find starting index of file in level_sst_infos_ that overlaps with
  // the fiels to be compacted from upper level
  std::optional<size_t> FindNonOverlappingFiles(int level,
                                                std::string_view smallest_key,
                                                std::string_view largest_key);

  // Execute compaction based on compact info
  void DoCompactJob();

  const Version *version_;

  VersionEdit *version_edit_;

  // NO need to acquire lock to protect this data structure. Because
  // new version is created when there is a change(create new SST, delete old
  // SST after compaction). So, each version has its own this data structure.
  // Note: These are also files that be deleted after finish compaction
  std::vector<const SSTMetadata *> files_need_compaction_[2];
};

} // namespace db

} // namespace kvs

#endif // DB_COMPACT_H