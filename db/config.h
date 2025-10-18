#ifndef DB_CONFIG_H
#define DB_CONFIG_H

#include <string>

namespace kvs {

namespace db {

class Config {
public:
  Config() = default;
  // For testing
  // TODO(namnh) : I don't like this design!!!
  Config(bool test_config, bool invalid_config = false);

  ~Config() = default;

  // No copy allowed
  Config(const Config &) = delete;
  Config &operator=(Config &) = delete;

  // Move constructor/assignment
  Config(Config &&) = default;
  Config &operator=(Config &&) = default;

  void LoadConfig();

  size_t GetPerMemTableSizeLimit() const;

  int GetMaxImmuMemTablesInMem() const;

  size_t GetSSTBlockSize() const;

  int GetSSTNumLvels() const;

  int GetLvl0SSTCompactionTrigger() const;

  std::string GetSavedDataPath() const;

  int GetTotalTablesCache() const;

  int GetTotalBlocksCache() const;

private:
  bool LoadConfigFromPath();

  void LoadDefaultConfig();

  size_t lsm_per_mem_size_limit_;

  int max_immutable_memtables_in_mem_;

  size_t sst_block_size_;

  int lsm_sst_num_levels_;

  int lvl0_compaction_trigger_;

  std::string data_path_;

  int total_tables_in_mem_;

  int total_blocks_in_mem_;

  // For testing
  bool is_testing_;
  bool invalid_config_;
};

} // namespace db

} // namespace kvs

#endif // DB_CONFIG_H