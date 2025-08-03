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
  skip_list->PrintSkipList();

  EXPECT_TRUE(skip_list->Delete("k1", 0));
  EXPECT_TRUE(!skip_list->Get("k1", 0).has_value());
  skip_list->PrintSkipList();

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

TEST(SkipListTest, GetAllPrefixes) {
  auto skip_list = std::make_unique<kvs::SkipList>();

  std::vector<std::pair<std::string, std::string>> pairs = {
      {"apple", "v1"}, {"application", "v2"}, {"angel", "v3"},
      {"Ana", "v4"},   {"Banana", "v5"},
  };

  for (const auto pair : pairs) {
    skip_list->Put(pair.first, pair.second, 0);
  }

  std::vector<std::string> values;
  values = skip_list->GetAllPrefixes("ap", 0);
  EXPECT_EQ(values.size(), 2);
  for (int i = 0; i < values.size(); i++) {
    EXPECT_EQ(skip_list->Get(pairs[i].first, 0).value(), values[i]);
  }

  values.clear();

  values = skip_list->GetAllPrefixes("Ap", 0);
  EXPECT_TRUE(values.empty());

  values = skip_list->GetAllPrefixes("A", 0);
  EXPECT_EQ(values.size(), 1);
  EXPECT_EQ(values[0], "v4");
}

TEST(SkipListTest, LargeScalePutAndGet) {
  auto skip_list = std::make_unique<kvs::SkipList>();

  const int num_keys = 100000;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    skip_list->Put(key, value, 0);
  }

  std::optional<std::string> val;
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    val = skip_list->Get(key, 0);
    EXPECT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), value);
  }
}

TEST(SkipListTest, LargeScaleDelete) {
  auto skip_list = std::make_unique<kvs::SkipList>();

  const int num_keys = 100000;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    skip_list->Put(key, value, 0);
  }

  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    EXPECT_TRUE(skip_list->Delete(key, 0));
  }

  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    EXPECT_FALSE(skip_list->Get(key, 0).has_value());
  }
}