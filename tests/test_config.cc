#include <gtest/gtest.h>

#include "db/config.h"

// libC++
#include <filesystem>
#include <memory>

namespace fs = std::filesystem;

namespace kvs {

namespace db {

TEST(ConfigTest, LoadConfigFromPathSuccess) {
  auto config = std::make_unique<db::Config>(true /*is_testing*/);
  config->LoadConfig();

  EXPECT_EQ(config->GetPerMemTableSizeLimit(), 33554432 /*4MB*/);
  EXPECT_EQ(config->GetMaxImmuMemTablesInMem(), 4);
  EXPECT_EQ(config->GetSSTBlockSize(), 4096 /*4KB*/);
  EXPECT_EQ(config->GetSSTNumLvels(), 7);
  EXPECT_EQ(config->GetLvl0SSTCompactionTrigger(), 6);
  // EXPECT_EQ(config->GetSavedDataPath(), "/var/lib/lsm-kv-storage/data");
}

} // namespace db

} // namespace kvs