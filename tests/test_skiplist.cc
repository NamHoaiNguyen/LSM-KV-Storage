#include <gtest/gtest.h>

#include "db/skiplist.h"

#include <memory>

TEST(CommonTest, LoggerBasic) {
  auto skip_list = std::make_unique<kvs::SkipList>();
  skip_list->PrintSkipList();
}