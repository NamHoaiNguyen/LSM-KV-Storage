#include "db/config.h"

#define TOML_EXCEPTIONS 0
#include "third_party/toml.hpp"

// libC++
#include <filesystem>

namespace fs = std::filesystem;

namespace {
constexpr size_t kDefaultMemtableSizeLimit = 4 * 1024 * 1024; // 4MB

constexpr int kDefaultMaxImmuMemtablesInMemory = 2;

constexpr size_t KDefaultSSTBlockSize = 4 * 1024; // 4KB

constexpr int kDefaultSSTNumLevels = 7;

constexpr int kDefaultLvl0CompactionTrigger = 4;

} // namespace

namespace kvs {

namespace db {

Config::Config(bool is_testing, bool invalid_config)
    : is_testing_(is_testing), invalid_config_(invalid_config) {}

void Config::LoadConfig() {
  if (!LoadConfigFromPath()) {
    LoadDefaultConfig();
  }
}

bool Config::LoadConfigFromPath() {
  fs::path exe_dir = fs::current_path();
  fs::path project_dir = exe_dir.parent_path();
  std::string config_path;
  if (is_testing_) {
    if (invalid_config_) {
      config_path = (project_dir / "tests/test_invalid_config.toml").string();
    } else {
      config_path = (project_dir / "tests/test_config.toml").string();
    }
  } else {
    config_path = (project_dir / "config/config.toml").string();
  }

  toml::parse_result result = toml::parse_file(config_path);

  if (!result) {
    return false;
  }

  toml::table tbl = result.table();

  if (!result["lsm"]["LSM_PER_MEM_SIZE_LIMIT"].as_integer()) {
    return false;
  }
  // Safe. Because limit of memtable size is less than 4 bytes
  lsm_per_mem_size_limit_ = static_cast<size_t>(
      result["lsm"]["LSM_PER_MEM_SIZE_LIMIT"].as_integer()->get());
  if (lsm_per_mem_size_limit_ < kDefaultMemtableSizeLimit ||
      lsm_per_mem_size_limit_ > kDefaultMemtableSizeLimit * 16 /*64MB*/) {
    return false;
  }
  if (!result["lsm"]["MAX_IMMUTABLE_MEMTABLES_IN_MEMORY"].as_integer()) {
    return false;
  }

  // Safe. Because limit of block size is less than 4 bytes
  max_immutable_memtables_in_mem_ = static_cast<int>(
      result["lsm"]["MAX_IMMUTABLE_MEMTABLES_IN_MEMORY"].as_integer()->get());
  if (max_immutable_memtables_in_mem_ <= 0 ||
      max_immutable_memtables_in_mem_ > 16) {
    return false;
  }

  if (!result["lsm"]["SST_BLOCK_SIZE"].as_integer()) {
    return false;
  }

  // Safe. Because limit of block size is less than 4 bytes
  sst_block_size_ =
      static_cast<size_t>(result["lsm"]["SST_BLOCK_SIZE"].as_integer()->get());
  if (sst_block_size_ < KDefaultSSTBlockSize ||
      sst_block_size_ > 8 * KDefaultSSTBlockSize /*32KB*/) {
    return false;
  }

  if (!result["lsm"]["LSM_SST_NUM_LEVELS"].as_integer()) {
    return false;
  }
  // Safe. Because limit of block size is less than 4 bytes
  lsm_sst_num_levels_ =
      static_cast<int>(result["lsm"]["LSM_SST_NUM_LEVELS"].as_integer()->get());
  if (lsm_sst_num_levels_ <= 0 || lsm_sst_num_levels_ > kDefaultSSTNumLevels) {
    return false;
  }

  if (!result["lsm"]["LVL0_COMPACTION_TRIGGER"].as_integer()) {
    return false;
  }
  // Safe. Because limit of block size is less than 4 bytes
  lvl0_compaction_trigger_ = static_cast<int>(
      result["lsm"]["LVL0_COMPACTION_TRIGGER"].as_integer()->get());
  if (lvl0_compaction_trigger_ <= 0 || lvl0_compaction_trigger_ > 8) {
    return false;
  }

  if (!result["lsm"]["DATA_PATH"].as_string()) {
    return false;
  }
  // Safe. Because limit of block size is less than 4 bytes
  // data_path_ = result["lsm"]["DATA_PATH"].as_string()->get();
  data_path_ = project_dir.string() + "/data/";
  if (data_path_.empty()) {
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

  fs::path exe_dir = fs::current_path();
  fs::path project_dir = exe_dir.parent_path();

  data_path_ = (project_dir / "data/").string();
}

size_t Config::GetPerMemTableSizeLimit() const {
  return lsm_per_mem_size_limit_;
}

int Config::GetMaxImmuMemTablesInMem() const {
  return max_immutable_memtables_in_mem_;
}

size_t Config::GetSSTBlockSize() const { return sst_block_size_; }

int Config::GetSSTNumLvels() const { return lsm_sst_num_levels_; }

int Config::GetLvl0SSTCompactionTrigger() const {
  return lvl0_compaction_trigger_;
}

std::string Config::GetSavedDataPath() const { return data_path_; }

} // namespace db

} // namespace kvs