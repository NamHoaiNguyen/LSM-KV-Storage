#include <gtest/gtest.h>

#include "db/skiplist.h"
#include "db/skiplist_iterator.h"
#include "db/status.h"

#include <algorithm>
#include <memory>
#include <unordered_set>

namespace kvs {

namespace db {

TEST(SkipListTest, BasicOperations) {
  auto skip_list = std::make_unique<db::SkipList>();

  // Add new key/value
  skip_list->Put("k1", "v1", 0);
  EXPECT_TRUE(skip_list->Get("k1", 0).type == db::ValueType::PUT);
  EXPECT_EQ(skip_list->Get("k1", 0).value, "v1");

  // Add new key/value
  skip_list->Put("k3", "v3", 0);
  EXPECT_TRUE(skip_list->Get("k3", 0).type == db::ValueType::PUT);
  EXPECT_EQ(skip_list->Get("k3", 0).value, "v3");

  skip_list->Put("k2", "v2", 0);
  EXPECT_TRUE(skip_list->Get("k2", 0).type == db::ValueType::PUT);
  EXPECT_EQ(skip_list->Get("k2", 0).value, "v2");

  skip_list->Delete("k2", 0);
  EXPECT_TRUE(skip_list->Get("k2", 0).type == db::ValueType::DELETED);
  EXPECT_TRUE(skip_list->Get("k2", 0).value == std::nullopt);

  skip_list->Delete("k1", 0);
  EXPECT_TRUE(skip_list->Get("k1", 0).type == db::ValueType::DELETED);
  EXPECT_TRUE(skip_list->Get("k1", 0).value == std::nullopt);

  skip_list->Delete("k3", 0);
  EXPECT_TRUE(skip_list->Get("k1", 0).type == db::ValueType::DELETED);
  EXPECT_TRUE(skip_list->Get("k1", 0).value == std::nullopt);
}

TEST(SkipListTest, DuplicatePut) {
  auto skip_list = std::make_unique<db::SkipList>();

  skip_list->Put("k1", "v1", 0);
  skip_list->Put("k1", "v2", 0);
  skip_list->Put("k1", "v3", 0);

  // When PUT a new value for a key which existed before,
  // value of key will be updated instead of creating a new one.
  EXPECT_EQ(skip_list->Get("k1", 0).value, "v3");
  EXPECT_TRUE(skip_list->Get("k1", 0).type == db::ValueType::PUT);
}

TEST(SkipListTest, BatchOperations) {
  auto skip_list = std::make_unique<db::SkipList>();
  const int num_keys = 100000;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, 100); // Range: 0 to 100

  std::vector<std::pair<std::string, std::string>> pairs;
  std::vector<std::string_view> keys_view;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    int randomNumber = distr(gen);

    key = "key" + std::to_string(randomNumber);
    value = "value" + std::to_string(randomNumber);

    pairs.push_back({key, value});
  }

  std::vector<std::pair<std::string_view, std::string_view>> pairs_view;
  for (int i = 0; i < num_keys; i++) {
    pairs_view.push_back(pairs[i]);
  }
  skip_list->BatchPut(pairs_view, 0 /*txn_id*/);

  for (const auto &pair : pairs) {
    keys_view.push_back(pair.first);
  }

  // skip_list->PrintSkipList();

  std::vector<std::pair<std::string, db::GetStatus>> get_res;
  get_res = skip_list->BatchGet(keys_view, 0 /*txn_id*/);

  for (int i = 0; i < num_keys; i++) {
    EXPECT_EQ(pairs[i].first, get_res[i].first);
    EXPECT_EQ(pairs[i].second, get_res[i].second.value);
  }

  skip_list->BatchDelete(keys_view, 0 /*txn_id*/);

  // Skiplist should be empty now
  get_res.clear();
  get_res = skip_list->BatchGet(keys_view, 0 /*txn_id*/);

  for (int i = 0; i < num_keys; i++) {
    EXPECT_EQ(pairs[i].first, get_res[i].first);
    EXPECT_TRUE(get_res[i].second.type == db::ValueType::DELETED);
    EXPECT_TRUE(get_res[i].second.value == std::nullopt);
  }
}

TEST(SkipListTest, GetAllPrefixes) {
  auto skip_list = std::make_unique<db::SkipList>();

  std::vector<std::pair<std::string, std::string>> pairs = {
      {"apple", "v1"}, {"application", "v2"}, {"angel", "v3"},
      {"Ana", "v4"},   {"Banana", "v5"},
  };

  for (const auto pair : pairs) {
    skip_list->Put(pair.first, pair.second, 0);
  }

  std::vector<std::optional<std::string>> values;
  values = skip_list->GetAllPrefixes("ap", 0);
  EXPECT_EQ(values.size(), 2);
  for (int i = 0; i < values.size(); i++) {
    EXPECT_EQ(skip_list->Get(pairs[i].first, 0).type, db::ValueType::PUT);
    EXPECT_EQ(skip_list->Get(pairs[i].first, 0).value.value(), values[i]);
  }

  values.clear();

  values = skip_list->GetAllPrefixes("Ap", 0);
  EXPECT_TRUE(values.empty());

  values = skip_list->GetAllPrefixes("A", 0);
  EXPECT_EQ(values.size(), 1);
  EXPECT_EQ(values[0], "v4");
}

TEST(SkipListTest, LargeScalePutAndGet) {
  auto skip_list = std::make_unique<db::SkipList>();

  const int num_keys = 100000;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, 100); // Range: 0 to 100

  std::vector<std::pair<std::string, std::string>> pairs;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    int randomNumber = distr(gen);

    key = "key" + std::to_string(randomNumber);
    value = "value" + std::to_string(randomNumber);
    skip_list->Put(key, value, 0);

    pairs.push_back({key, value});
  }

  for (int i = 0; i < num_keys; i++) {
    GetStatus status = skip_list->Get(pairs[i].first, 0);

    EXPECT_TRUE(status.type == db::ValueType::PUT);
    EXPECT_EQ(status.value, pairs[i].second);
  }

  // put same key but different values
  for (int i = 0; i < num_keys; i++) {
    key = pairs[i].first;
    value = "value" + std::to_string(i + num_keys);
    skip_list->Put(key, value, 0);

    pairs.push_back({key, value});
  }

  std::reverse(pairs.begin(), pairs.end());

  std::unordered_set<std::string> seen;
  std::vector<std::pair<std::string, std::string>> result;

  for (const auto &p : pairs) {
    // Only keep the key that appear first
    if (seen.find(p.first) == seen.end()) {
      result.push_back(p);
      seen.insert(p.first);
    }
  }

  for (int i = 0; i < 2 * num_keys; i++) {
    // Skiplist now contains multiple same keys but different values.
    // But it should only return the latest key/value put pair
    key = pairs[i].first;
    value = "value" + std::to_string(i + num_keys);

    GetStatus status = skip_list->Get(key, 0);

    EXPECT_TRUE(status.type == db::ValueType::PUT);
    bool found =
        std::any_of(result.begin(), result.end(), [status](const auto &val) {
          return status.value == val.second;
        });
    EXPECT_TRUE(found);
  }
}

TEST(SkipListTest, LargeScaleOnlyDelete) {
  auto skip_list = std::make_unique<db::SkipList>();

  const int num_keys = 100000;
  std::string key{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    skip_list->Delete(key, 0 /*txn_id*/);
  }

  for (int i = 0; i < num_keys; i++) {
    EXPECT_TRUE(skip_list->Get(key, 0 /*txn_id*/).type ==
                db::ValueType::DELETED);
    EXPECT_TRUE(skip_list->Get(key, 0 /*txn_id*/).value == std::nullopt);
  }
}

TEST(SkipListTest, LargeScaleDelete) {
  auto skip_list = std::make_unique<db::SkipList>();

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

    skip_list->Delete(key, 0);
  }

  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    EXPECT_TRUE(skip_list->Get(key, 0).type == db::ValueType::DELETED);
    EXPECT_TRUE(skip_list->Get(key, 0).value == std::nullopt);
  }
}

TEST(SkipListTest, Iterator) {
  auto skip_list = std::make_unique<db::SkipList>();

  const int num_keys = 10;
  std::string key{}, value{};
  for (int i = 0; i < num_keys; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);
    skip_list->Put(key, value, 0);
  }

  auto iter = std::make_unique<db::SkipListIterator>(skip_list.get());
  int count = 0;
  for (iter->SeekToFirst(); iter->IsValid(); iter->Next()) {
    key = "key" + std::to_string(count);
    value = "value" + std::to_string(count);
    EXPECT_EQ(iter->GetKey(), key);
    EXPECT_EQ(iter->GetValue(), value);
    count++;
  }
}

} // namespace db

} // namespace kvs
