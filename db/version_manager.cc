#include "db/version_manager.h"

#include "common/base_iterator.h"
#include "db/compact.h"
#include "db/version.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "sstable/block.h"
#include "sstable/block_index.h"
#include "sstable/sst.h"

namespace kvs {

namespace db {

VersionManager::VersionManager(DBImpl *db, const Config *config)
    : db_(db), config_(config) {}

Version *VersionManager::CreateNewVersion() {
  versions_.push_front(std::move(latest_version_));
  latest_version_ = std::make_unique<Version>(db_, config_);

  return latest_version_.get();
}

} // namespace db

} // namespace kvs