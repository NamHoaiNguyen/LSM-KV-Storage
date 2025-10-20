#include <gtest/gtest.h>

#include "common/macros.h"
#include "db/base_memtable.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "db/merge_iterator.h"
#include "db/status.h"
#include "db/version.h"
#include "db/version_manager.h"
#include "io/linux_file.h"
#include "sstable/block_builder.h"
#include "sstable/block_index.h"
#include "sstable/block_reader.h"
#include "sstable/block_reader_iterator.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_iterator.h"

// libC++
#include <algorithm>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <thread>

namespace fs = std::filesystem;

namespace kvs {

namespace db {

bool CompareVersionFilesWithDirectoryFiles(const db::DBImpl *db) {
  int num_sst_files = 0;
  int num_sst_files_info = 0;

  for (const auto &entry : fs::directory_iterator(db->GetDBPath())) {
    if (fs::is_regular_file(entry.status())) {
      num_sst_files++;
    }
  }

  for (const auto &sst_file_info :
       db->GetVersionManager()->GetLatestVersion()->GetImmutableSSTMetadata()) {
    num_sst_files_info += sst_file_info.size();
  }

  return (num_sst_files == num_sst_files_info + 1 /*include manifest file*/)
             ? true
             : false;
}

void ClearAllSstFiles(const db::DBImpl *db) {
  // clear all SST files created for next test
  for (const auto &entry : fs::directory_iterator(db->GetDBPath())) {
    if (fs::is_regular_file(entry.status())) {
      fs::remove(entry.path());
    }
  }
}

TEST(TableTest, MergeIterator) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB("test");

  const db::Config *const config = db->GetConfig();
  // That number of key/value pairs is enough to create a new sst
  const int nums_elem = 10000000;

  std::string key, value;
  int immutable_memtables_in_mem = 0, current_size = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, 1000); // Range: 0 to 1000

  std::vector<std::pair<std::string, std::string>> list_key_value;

  for (int i = 0; i < nums_elem; i++) {
    // int randomNumber = distr(gen);
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    db->Put(key, value, 0 /*txn_id*/);
    list_key_value.push_back({std::string(key), std::string(value)});

    current_size += key.size() + value.size();
    if (current_size >= config->GetPerMemTableSizeLimit()) {
      current_size = 0;
      immutable_memtables_in_mem++;
      if (immutable_memtables_in_mem >= config->GetMaxImmuMemTablesInMem()) {
        // Stop immediately if flushing is triggered
        break;
      }
    }
  }

  // Force flushing immutable memtable to disk
  // db->ForceFlushMemTable();

  std::sort(list_key_value.begin(), list_key_value.end());

  // // Need time for new SST is persisted to disk
  // // NOTE: It must be long enough for debug build
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  EXPECT_TRUE(CompareVersionFilesWithDirectoryFiles(db.get()));

  const std::vector<std::vector<std::shared_ptr<db::SSTMetadata>>>
      sst_metadata = db->GetVersionManager()
                         ->GetLatestVersion()
                         ->GetImmutableSSTMetadata();
  EXPECT_EQ(sst_metadata.size(), db->GetConfig()->GetSSTNumLvels());

  for (int level = 0; level < sst_metadata.size(); level++) {
    if (level == 0) {
      EXPECT_EQ(sst_metadata[level].size(), config->GetMaxImmuMemTablesInMem());
    } else {
      EXPECT_EQ(sst_metadata[level].size(), 0);
    }
  }

  // std::vector<std::unique_ptr<sstable::LRUTableItem>> lru_table_items;
  std::vector<std::shared_ptr<sstable::LRUTableItem>> lru_table_items;
  std::vector<std::unique_ptr<sstable::TableReaderIterator>>
      table_reader_iterators;
  for (int i = 0; i < sst_metadata[0].size(); i++) {
    std::string filename =
        db->GetDBPath() + std::to_string(sst_metadata[0][i]->table_id) + ".sst";
    std::unique_ptr<sstable::TableReader> table_reader =
        sstable::CreateAndSetupDataForTableReader(
            std::move(filename), sst_metadata[0][i]->table_id,
            sst_metadata[0][i]->file_size);

    // Mock LRU table item
    // auto lru_table_item = std::make_unique<sstable::LRUTableItem>(
    //     sst_metadata[0][i]->table_id /*table_id*/, std::move(table_reader),
    //     db->GetTableReaderCache());

    auto lru_table_item = std::make_shared<sstable::LRUTableItem>(
        sst_metadata[0][i]->table_id /*table_id*/, std::move(table_reader),
        db->GetTableReaderCache());

    auto iterator = std::make_unique<sstable::TableReaderIterator>(
        db->GetBlockReaderCache(), lru_table_item);

    lru_table_items.push_back(std::move(lru_table_item));
    table_reader_iterators.push_back(std::move(iterator));
  }

  auto iterator =
      std::make_unique<MergeIterator>(std::move(table_reader_iterators));

  int total_elems = 0;
  // Forward traverse
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    std::string_view key_found = iterator->GetKey();
    std::string_view value_found = iterator->GetValue();

    // Order of key/value in iterator must be sorted
    EXPECT_EQ(key_found, list_key_value[total_elems].first);
    EXPECT_EQ(value_found, list_key_value[total_elems].second);

    total_elems++;
  }

  int last_elem_index = total_elems - 1;
  // Backward traverse
  for (iterator->SeekToLast(); iterator->IsValid(); iterator->Prev()) {
    std::string_view key_found = iterator->GetKey();
    std::string_view value_found = iterator->GetValue();

    // Order of key/value in iterator must be sorted
    EXPECT_EQ(key_found, list_key_value[last_elem_index].first);
    EXPECT_EQ(value_found, list_key_value[last_elem_index].second);

    last_elem_index--;
  }

  // Number of key value pairs should be equal to list_key_value's size.
  EXPECT_EQ(list_key_value.size(), total_elems);

  ClearAllSstFiles(db.get());
}

} // namespace db

} // namespace kvs