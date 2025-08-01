#include <gtest/gtest.h>

#include "db/skiplist.h"

#include <memory>

TEST(SkipListTest, BasicOperations) {
  auto skip_list = std::make_unique<kvs::SkipList>();

  // Add new key/value
  skip_list->Put("k1", "v1", 0);
  EXPECT_EQ(skip_list->Get("k1", 0).value(), "v1");

  // Add new key/value
  skip_list->Put("k3", "v3", 0);
  EXPECT_EQ(skip_list->Get("k3", 0).value(), "v3");

  skip_list->Put("k2", "v2", 0);
  EXPECT_EQ(skip_list->Get("k2", 0).value(), "v2");

  skip_list->PrintSkipList();

  EXPECT_TRUE(skip_list->Delete("k2", 0));
  EXPECT_TRUE(!skip_list->Get("k2", 0).has_value());

  EXPECT_TRUE(skip_list->Delete("k1", 0));
  EXPECT_TRUE(!skip_list->Get("k1", 0).has_value());

  EXPECT_TRUE(skip_list->Delete("k3", 0));
  EXPECT_TRUE(!skip_list->Get("k3", 0).has_value());

  skip_list->PrintSkipList();
}

TEST(SkipListTest, DuplicatePut) {
  auto skip_list = std::make_unique<kvs::SkipList>();

  skip_list->Put("k1", "v1", 0);
  skip_list->Put("k1", "v2", 0);
  skip_list->Put("k1", "v3", 0);

  // When PUT a new value for a key which existed before,
  // value of key will be updated instead of creating a new one.
  EXPECT_EQ(skip_list->Get("k1", 0).value(), "v3");
}

TEST(SkipListTest, BasicLeakFree) {
  auto skip_list = std::make_unique<kvs::SkipList>();
  skip_list->Put("k1", "v1", 0);
  skip_list->Put("k2", "v2", 0);

  EXPECT_TRUE(skip_list->Delete("k1", 0));
  EXPECT_TRUE(!skip_list->Get("k1", 0).has_value());
  // EXPECT_EQ(skip_list->CheckNodeRefCount(), 1);

  EXPECT_TRUE(skip_list->Delete("k2", 0));
  EXPECT_TRUE(!skip_list->Get("k2", 0).has_value());
  // EXPECT_EQ(skip_list->CheckNodeRefCount(), 1);
}