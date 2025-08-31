#include "db/config.h"

#define TOML_EXCEPTIONS 0
#include "third_party/toml.hpp"

// libC++
#include <filesystem>

namespace fs = std::filesystem;

namespace {
constexpr long long kDefaultMemtableSizeLimit = 4 * 1024 * 1024; // 4MB

constexpr int kDefaultMaxImmuMemtablesInMemory = 2;

constexpr int KDefaultSSTBlockSize = 4 * 1024; // 4KB

constexpr int kDefaultSSTNumLevels = 7;

constexpr int kDefaultLvl0CompactionTrigger = 4;
} // namespace

namespace kvs {

namespace db {

void Config::LoadConfig() {
  if (!LoadConfigFromPath()) {
    LoadDefaultConfig();
  }
}

bool Config::LoadConfigFromPath() {
  fs::path exe_dir = fs::current_path();
  fs::path project_dir = exe_dir.parent_path();
  std::string config_path = (project_dir / "config.toml").string();

  toml::parse_result result = toml::parse_file(config_path);

  if (!result) {
    return false;
  }

  toml::table tbl = result.table();

  if (!result["lsm"]["LSM_PER_MEM_SIZE_LIMIT"].as_integer()) {
    return false;
  }
  lsm_per_mem_size_limit_ =
      result["lsm"]["LSM_PER_MEM_SIZE_LIMIT"].as_integer()->get();
  if (lsm_per_mem_size_limit_ < kDefaultMemtableSizeLimit) {
    return false;
  }
  if (!result["lsm"]["MAX_IMMUTABLE_MEMTABLES_IN_MEMORY"].as_integer()) {
    return false;
  }

  max_immutable_memtables_in_mem_ =
      result["lsm"]["MAX_IMMUTABLE_MEMTABLES_IN_MEMORY"].as_integer()->get();
  if (max_immutable_memtables_in_mem_ <= 0 ||
      max_immutable_memtables_in_mem_ > 16) {
    return false;
  }

  if (!result["lsm"]["SST_BLOCK_SIZE"].as_integer()) {
    return false;
  }
  sst_block_size_ = result["lsm"]["SST_BLOCK_SIZE"].as_integer()->get();
  if (sst_block_size_ < KDefaultSSTBlockSize) {
    return false;
  }

  if (!result["lsm"]["LSM_SST_NUM_LEVELS"].as_integer()) {
    return false;
  }
  lsm_sst_num_levels_ = result["lsm"]["LSM_SST_NUM_LEVELS"].as_integer()->get();
  if (lsm_sst_num_levels_ <= 0 || lsm_sst_num_levels_ > kDefaultSSTNumLevels) {
    return false;
  }

  lvl0_compaction_trigger_ =
      result["lsm"]["LVL0_COMPACTION_TRIGGER"].as_integer()->get();
  if (lvl0_compaction_trigger_ <= 0 || lvl0_compaction_trigger_ > 8) {
    return false;
  }

  return true;
}

void Config::LoadDefaultConfig() {
  lsm_per_mem_size_limit_ = kDefaultMemtableSizeLimit;
  max_immutable_memtables_in_mem_ = kDefaultMaxImmuMemtablesInMemory;
  sst_block_size_ = KDefaultSSTBlockSize;
  lsm_sst_num_levels_ = kDefaultSSTNumLevels;
  lvl0_compaction_trigger_ = kDefaultLvl0CompactionTrigger;
}

const long long Config::GetPerMemTableSizeLimit() const {
  return lsm_per_mem_size_limit_;
}

const int Config::GetMaxImmuMemTablesInMem() const {
  return max_immutable_memtables_in_mem_;
}

const int Config::GetSSTBlockSize() const { return sst_block_size_; }

const int Config::GetSSTNumLvels() const { return lsm_sst_num_levels_; }

const int Config::GetLvl0SSTCompactionTrigger() const {
  return lvl0_compaction_trigger_;
}

} // namespace db

} // namespace kvs