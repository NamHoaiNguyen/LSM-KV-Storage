#include <gtest/gtest.h>

#include "db/skiplist.h"

#include <memory>

TEST(CommonTest, BasicOperations) {
  auto skip_list = std::make_unique<kvs::SkipList>();

  // Add new key/value
  skip_list->Put("k1", "v1", 0);
  EXPECT_EQ(skip_list->Get("k1", 0).value(), "v1");

  // Add new key/value
  skip_list->Put("k3", "v3", 0);
  EXPECT_EQ(skip_list->Get("k3", 0).value(), "v3");

  skip_list->PrintSkipList();

  // Update key with new value
  skip_list->Put("k1", "v1_new", 0);
  EXPECT_EQ(skip_list->Get("k1", 0).value(), "v1_new");

  skip_list->Put("k2", "v2", 0);
  EXPECT_EQ(skip_list->Get("k2", 0).value(), "v2");

  skip_list->PrintSkipList();
}