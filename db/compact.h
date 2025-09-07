#ifndef DB_COMPACT_H
#define DB_COMPACT_H

#include "version.h"

#include <cstdint>
#include <string_view>
#include <vector>

namespace kvs {

namespace db {

// KEY RULE: Compact is only triggerd by LATEST version
class Compact {
public:
  explicit Compact(const Version *version);

  ~Compact() = default;

  // Copy constructor/movement
  Compact(Compact &&) = default;
  Compact &operator=(Compact &&) = default;

  // sst_lvl0_size works like a snapshot of SSTInfo size at the time that
  // compact is triggered
  void PickCompact(int sst_lvl0_size);

private:
  void DoL0L1LvlCompact();

  // Find all overlapping sst files at level
  std::pair<std::string_view, std::string_view> GetOverlappingSSTLvl0();

  void GetOverlappingSSTOtherLvls(uint8_t level, std::string_view smallest_key,
                                  std::string_view largest_key);

  // Find starting index of file in level_sst_infos_ that overlaps with
  // the fiels to be compacted from upper level
  size_t FindNonOverlappingFiles(uint8_t level, std::string_view smallest_key,
                                 std::string_view largest_key);

  void DoCompactJob();

  const Version *version_;

  // NO need to acquire lock to protect this data structure. Because
  // new version is created when there is a change(create new SST, delete old
  // SST after compaction). So, each version has its own this data structure.
  // Note: These are also files that be deleted after finish compaction
  std::vector<const Version::SSTInfo *> compact_info_;
};

} // namespace db

} // namespace kvs

#endif // DB_COMPACT_H