#ifndef DB_VERSION_EDIT_H
#define DB_VERSION_EDIT_H

#include "common/macros.h"
#include "sstable/block_index.h"

#include "sstable/table_builder.h"

// libC++
#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace kvs {

namespace db {

struct SSTMetadata {
  SSTMetadata(SSTId table_id_, int level_, uint64_t file_size_,
              std::string_view smallest_key_, std::string_view largest_key_,
              std::string &&filename_);

  ~SSTMetadata() = default;

  // No copy allowed
  SSTMetadata(const SSTMetadata &) = delete;
  SSTMetadata &operator=(SSTMetadata &) = delete;

  // No move allowed
  SSTMetadata(SSTMetadata &&) = delete;
  SSTMetadata &operator=(SSTMetadata &&) = delete;

  const SSTId table_id;

  const std::string filename;

  const int level;

  const uint64_t file_size;

  const std::string smallest_key;

  const std::string largest_key;

  std::atomic<uint64_t> ref_count;
};

class VersionEdit {
public:
  explicit VersionEdit(int num_levels);

  ~VersionEdit() = default;

  // No copy allowed
  VersionEdit(const VersionEdit &) = delete;
  VersionEdit &operator=(VersionEdit &) = delete;

  // Move constructor/assignment
  VersionEdit(VersionEdit &&) = default;
  VersionEdit &operator=(VersionEdit &&) = default;

  void AddNewFiles(SSTId table_id, int level, uint64_t file_size,
                   std::string_view smallest_key, std::string_view largest_key,
                   std::string &&filename);

  void AddNewFiles(std::shared_ptr<SSTMetadata> sst_metadata);

  // Add files that need to be deleted in new version
  void RemoveFiles(SSTId sst_id, int level);

  // Set Next table Number
  void SetNextTableId(uint64_t next_table_id);

  const std::set<std::pair<SSTId, int>> &GetImmutableDeletedFiles() const;

  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &
  GetImmutableNewFiles() const;

  uint64_t GetNextTableId() const;

private:
  // SST id + level
  std::set<std::pair<SSTId, int>> deleted_files_;

  std::vector<std::vector<std::shared_ptr<SSTMetadata>>> new_files_;

  uint64_t next_table_id_{0};
};

} // namespace db

} // namespace kvs

#endif // DB_VERSION_EDIT_H