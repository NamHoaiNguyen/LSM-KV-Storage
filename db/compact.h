#ifndef DB_COMPACT_H
#define DB_COMPACT_H

#include "version.h"
#include "version_edit.h"

#include <cstdint>
#include <string_view>
#include <vector>

namespace kvs {

class BaseIterator;

namespace sstable {
class BlockReaderCache;
class TableReaderCache;
} // namespace sstable

namespace db {

class MergeIterator;

class DBImpl;

// KEY RULE: Compact is only triggerd by LATEST version
class Compact {
public:
  // Compact(const sstable::BlockReaderCache *block_reader_cache,
  //         const sstable::TableReaderCache *table_reader_cache,
  //         const Version *version, VersionEdit *version_edit, DBImpl *db);

  Compact(const std::vector<std::unique_ptr<sstable::BlockReaderCache>>
              &block_reader_cache,
          const sstable::TableReaderCache *table_reader_cache,
          const Version *version, VersionEdit *version_edit, DBImpl *db);

  ~Compact() = default;

  // Copy constructor/assignment
  Compact(const Compact &) = delete;
  Compact &operator=(Compact &) = delete;

  // Move constructor/assignment
  Compact(Compact &&) = delete;
  Compact &operator=(Compact &&) = delete;

  // sst_lvl0_size works like a snapshot of SSTInfo size at the time that
  // compact is triggered
  bool PickCompact();

private:
  bool DoL0L1Compact();

  std::unique_ptr<MergeIterator> CreateMergeIterator();

  // Find all overlapping sst files at level
  std::pair<std::string_view, std::string_view>
  GetOverlappingSSTLvl0(std::string_view smallest_key,
                        std::string_view largest_key, int oldest_sst_index);

  // Find all SSTs at next level that overllap with previous level
  void GetOverlappingSSTNextLvl(int level, std::string_view smallest_key,
                                std::string_view largest_key);

  // Find starting index of file in level_sst_infos_ that overlaps  range from
  // smallest_key to largest_key
  std::optional<int64_t> FindFile(int level, std::string_view smallest_key,
                                  std::string_view largest_key);

  // Execute compaction based on compact info
  bool DoCompactJob();

  // Decide that a key should be kept or skipped
  bool ShouldKeepEntry(std::string_view last_current_key, std::string_view key,
                       TxnId last_txn_id, TxnId txn_id, ValueType type);

  // Check that if there are higher level(from level_to_compact_+ 2) have key or
  // not
  bool IsBaseLevelForKey(std::string_view key);

  // const sstable::BlockReaderCache *block_reader_cache_;

  const std::vector<std::unique_ptr<sstable::BlockReaderCache>>
      &block_reader_cache_;

  const sstable::TableReaderCache *table_reader_cache_;

  const Version *version_;

  DBImpl *db_;

  // NO need to acquire lock to protect this data structure. Because
  // new version is created when there is a change(create new SST, delete old
  // SST after compaction). So, each version has its own this data structure.
  // Note: These are also files that be deleted after finish compaction
  std::vector<const SSTMetadata *> files_need_compaction_[2];

  int level_to_compact_;

  VersionEdit *version_edit_;
};

} // namespace db

} // namespace kvs

#endif // DB_COMPACT_H