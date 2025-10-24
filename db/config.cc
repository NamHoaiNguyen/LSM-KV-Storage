#include "db/config.h"

#define TOML_EXCEPTIONS 0
#include "third_party/toml.hpp"

// libC++
#include <exception>
#include <filesystem>
#include <iostream>

namespace fs = std::filesystem;

namespace {
constexpr size_t kDefaultMemtableSizeLimit = 4 * 1024 * 1024; // 4MB

constexpr size_t KDefaultSSTBlockSize = 4 * 1024; // 4KB

constexpr int kDefaultSSTNumLevels = 7;

constexpr int kDefaultTotalTablesInMem = 1000;

} // namespace

namespace kvs {

namespace db {

Config::Config(bool is_testing, bool invalid_config)
    : is_testing_(is_testing), invalid_config_(invalid_config) {
  if (!LoadConfig()) {
    std::terminate();
  }
}

bool Config::LoadConfig() { return LoadConfigFromPath(); }

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
    std::cout << "Can't parse config file" << std::endl;
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
      lsm_per_mem_size_limit_ > kDefaultMemtableSizeLimit * 256 /*1GB*/) {
    std::cout << "Size of LSM must be in rage 4-1024MB" << std::endl;
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
    std::cout << "Number of immutable memtables isn't valid(1-16)" << std::endl;
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
    std::cout << "LSM_SST_NUM_LEVELS is not integer" << std::endl;
    return false;
  }
  // Safe. Because limit of block size is less than 4 bytes
  lsm_sst_num_levels_ =
      static_cast<int>(result["lsm"]["LSM_SST_NUM_LEVELS"].as_integer()->get());
  if (lsm_sst_num_levels_ <= 0 || lsm_sst_num_levels_ > kDefaultSSTNumLevels) {
    std::cout << "LSM_SST_NUM_LEVELS isn't valid(1-7)" << std::endl;
    return false;
  }

  if (!result["lsm"]["LVL0_COMPACTION_TRIGGER"].as_integer()) {
    return false;
  }
  // Safe. Because limit of block size is less than 4 bytes
  lvl0_compaction_trigger_ = static_cast<int>(
      result["lsm"]["LVL0_COMPACTION_TRIGGER"].as_integer()->get());
  if (lvl0_compaction_trigger_ <= 0 || lvl0_compaction_trigger_ > 8) {
    std::cout << "LVL0_COMPACTION_TRIGGER isn't valid(1-8)" << std::endl;
    return false;
  }

  data_path_ = project_dir.string() + "/data/";
  if (data_path_.empty()) {
    return false;
  }

  if (!result["cache"]["TOTAL_TABLES_CACHE"].as_integer()) {
    std::cout << "TOTAL_TABLES_CACHE is not integer" << std::endl;
    return false;
  }

  total_background_threads_ =
      static_cast<int>(result["cache"]["TOTAL_BG_THREADS"].as_integer()->get());
  if (total_background_threads_ <= 0) {
    std::cout << "TOTAL_BG_THREADS is not valid(>=1)" << std::endl;
    return false;
  }

  if (!result["cache"]["TOTAL_BG_THREADS"].as_integer()) {
    std::cout << "TOTAL_BG_THREADS is not integer" << std::endl;
    return false;
  }

  total_tables_in_mem_ = static_cast<int>(
      result["cache"]["TOTAL_TABLES_CACHE"].as_integer()->get());
  if (total_tables_in_mem_ < 0 ||
      total_tables_in_mem_ > kDefaultTotalTablesInMem) {
    std::cout << "LVL0_COMPACTION_TRIGGER isn't valid(1-1000)" << std::endl;
    return false;
  }

  if (!result["cache"]["TOTAL_BLOCKS_CACHE"].as_integer()) {
    std::cout << "TOTAL_BLOCKS_CACHE is not integer" << std::endl;
    return false;
  }

  total_blocks_in_each_cache_ = static_cast<int>(
      result["cache"]["TOTAL_BLOCKS_EACH_CACHE"].as_integer()->get());
  if (total_blocks_in_each_cache_ <= 0) {
    std::cout << "TOTAL_BLOCK_EACH_CACHE isn't valid(must be larger than 0)"
              << std::endl;
    return false;
  }

  total_block_caches_ = static_cast<int>(
      result["cache"]["TOTAL_BLOCKS_CACHE"].as_integer()->get());
  if (total_block_caches_ < 0) {
    std::cout << "TOTAL_BLOCKS_CACHE is not valid(>=0)" << std::endl;
    return false;
  }

  if (!result["cache"]["TOTAL_BLOCKS_CACHE"].as_integer()) {
    std::cout << "TOTAL_BLOCKS_CACHE is not integer" << std::endl;
    return false;
  }

  return true;
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

int Config::GetTotalBackGroundThreads() const {
  return total_background_threads_;
}

int Config::GetTotalTablesCache() const { return total_tables_in_mem_; }

int Config::GetTotalBlocksEachCache() const {
  return total_blocks_in_each_cache_;
}

int Config::GetTotalBlocksCache() const { return total_block_caches_; }

} // namespace db

} // namespace kvs