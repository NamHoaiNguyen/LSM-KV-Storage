#ifndef DB_VERSION_EDIT_H
#define DB_VERSION_EDIT_H

#include "common/macros.h"
#include "sstable/block_index.h"

#include "sstable/table_builder.h"

// libC++
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace kvs {

namespace db {

struct SSTMetadata {
  SSTId table_id;

  int level;

  uint64_t file_size;

  // TODO(namnh) : These two fields maybe are used in the future.(for table
  // cache)
  std::string smallest_key;

  std::string largest_key;
};

class VersionEdit {
public:
  VersionEdit() = default;

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

  // Add files that need to be deleted in new version
  void RemoveFiles(int level, SSTId sst_id);

  const std::set<std::pair<SSTId, int>> &GetImmutableDeletedFiles();

  const std::vector<std::shared_ptr<SSTMetadata>> &GetImmutableNewFiles();

private:
  // SST id + level
  std::set<std::pair<SSTId, int>> deleted_files_;

  std::vector<std::shared_ptr<SSTMetadata>> new_files_;
};

} // namespace db

} // namespace kvs

#endif // DB_VERSION_EDIT_H