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

  EXPECT_EQ(config->GetPerMemTableSizeLimit(), 4194304 /*4MB*/);
  EXPECT_EQ(config->GetMaxImmuMemTablesInMem(), 1);
  EXPECT_EQ(config->GetSSTBlockSize(), 4096 /*4KB*/);
  EXPECT_EQ(config->GetSSTNumLvels(), 7);
  EXPECT_EQ(config->GetLvl0SSTCompactionTrigger(), 2);
  // EXPECT_EQ(config->GetSavedDataPath(), "/var/lib/lsm-kv-storage/data");
}

TEST(ConfigTest, UseDefaultConfig) {
  auto config = std::make_unique<db::Config>(true /*is_testing*/,
                                             true /*invalid_config*/);
  // EXPECT_FALSE(config->LoadConfigFromPath());

  config->LoadConfig();
  EXPECT_EQ(config->GetPerMemTableSizeLimit(), 4194304 /*4MB*/);
  EXPECT_EQ(config->GetMaxImmuMemTablesInMem(), 1);
  EXPECT_EQ(config->GetSSTBlockSize(), 4096 /*4KB*/);
  EXPECT_EQ(config->GetSSTNumLvels(), 7);
  EXPECT_EQ(config->GetLvl0SSTCompactionTrigger(), 2);

  fs::path exe_dir = fs::current_path();
  fs::path project_dir = exe_dir.parent_path();
  EXPECT_EQ(config->GetSavedDataPath(), (project_dir / "data/").string());
}

} // namespace db

} // namespace kvs