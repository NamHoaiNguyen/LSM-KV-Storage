#ifndef DB_CONFIG_H
#define DB_CONFIG_H

#include <string>

namespace kvs {

namespace db {

class Config {
public:
  Config() = default;

  ~Config() = default;

  // No copy allowed
  Config(const Config &) = delete;
  Config &operator=(Config &) = delete;

  // Move constructor/alignment
  Config(Config &&) = default;
  Config &operator=(Config &&) = default;

  void LoadConfig();

  const long long GetPerMemTableSizeLimit() const;

  const int GetMaxImmuMemTablesInMem() const;

  const int GetSSTBlockSize() const;

  const int GetSSTNumLvels() const;

  const int GetLvl0SSTCompactionTrigger() const;

private:
  bool LoadConfigFromPath();

  void LoadDefaultConfig();

  long long lsm_per_mem_size_limit_;

  int max_immutable_memtables_in_mem_;

  int sst_block_size_;

  int lsm_sst_num_levels_;

  int lvl0_compaction_trigger_;
};

} // namespace db

} // namespace kvs

#endif // DB_CONFIG_H