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

  // Move constructor/alignment
  Config(Config &&) = default;
  Config &operator=(Config &&) = default;

  void LoadConfig();

  const size_t GetPerMemTableSizeLimit() const;

  const int GetMaxImmuMemTablesInMem() const;

  const size_t GetSSTBlockSize() const;

  const int GetSSTNumLvels() const;

  const int GetLvl0SSTCompactionTrigger() const;

  // Not best practises. But in this case, it is fine because this is never be
  // changed!!!
  std::string_view GetSavedDataPath() const;

  // For testing
  friend class ConfigTest_LoadConfigFromPathSuccess_Test;
  friend class ConfigTest_UseDefaultConfig_Test;

private:
  bool LoadConfigFromPath();

  void LoadDefaultConfig();

  size_t lsm_per_mem_size_limit_;

  int max_immutable_memtables_in_mem_;

  size_t sst_block_size_;

  int lsm_sst_num_levels_;

  int lvl0_compaction_trigger_;

  std::string data_path_;

  // For testing
  bool is_testing_;
  bool invalid_config_;
};

} // namespace db

} // namespace kvs

#endif // DB_CONFIG_H