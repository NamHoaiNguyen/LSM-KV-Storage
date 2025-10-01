#include <gtest/gtest.h>

#include "common/macros.h"
#include "db/config.h"
#include "db/db_impl.h"
#include "db/status.h"
#include "db/version.h"
#include "db/version_manager.h"
#include "io/linux_file.h"
#include "sstable/block_builder.h"
#include "sstable/block_index.h"
#include "sstable/block_reader.h"
#include "sstable/block_reader_iterator.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"

// libC++
#include <cmath>
#include <filesystem>
#include <memory>
#include <random>

namespace fs = std::filesystem;

namespace kvs {

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

TEST(BlockTest, BasicEncode) {
  std::vector<Byte> data_encoded = {
      // Data section
      // Entry 1
      0,                            // value_type = 0(not_deleted)
      5, 0, 0, 0,                   // key_len = 5
      'a', 'p', 'p', 'l', 'e',      // key
      6, 0, 0, 0,                   // value_len = 6
      'v', 'a', 'l', 'u', 'e', '1', // value
      0x39, 0x30, 0, 0, 0, 0, 0, 0, // transaction_id = 12345
      // Entry 2
      0,                                 // value_type = 0(not_deleted)
      5, 0, 0, 0,                        // key_len = 5
      'a', 'p', 'p', 'l', 'y',           // key
      7, 0, 0, 0,                        // value_len = 7
      's', 'u', 'c', 'c', 'e', 's', 's', // value
      0x94, 0x26, 0, 0, 0, 0, 0, 0,      // transaction_id = 9876
      // Entry 3
      0,                                      // value_type = 0(not_deleted)
      8, 0, 0, 0,                             // key_len = 8
      'c', 'o', 'l', 'o', 's', 's', 'u', 's', // key
      7, 0, 0, 0,                             // value_len = 7
      't', 'h', 'u', 'n', 'd', 'e', 'r',      // value
      0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0,     // transaction_id = 2^32-1
  };

  std::vector<Byte> offset_encoded = {
      // Offset entry 0
      0, 0, 0, 0, 0, 0, 0, 0,    // start_offset of data entry 0
      0x1C, 0, 0, 0, 0, 0, 0, 0, // length of data entry 0
      // Offset entry 1
      0x1C, 0, 0, 0, 0, 0, 0, 0, // start_offset of data entry 1
      0x1D, 0, 0, 0, 0, 0, 0, 0, // length of data entry 1
      // Offset entry 2
      0x39, 0, 0, 0, 0, 0, 0, 0, // start_offset of data entry 2
      0x20, 0, 0, 0, 0, 0, 0, 0, // length of data entry 2
  };

  std::vector<Byte> extra_encoded = {
      // Total entries
      0x03,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // Stating offset of offset section
      0x59,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
  };

  auto block = std::make_unique<sstable::BlockBuilder>();

  std::string key1 = "apple";
  std::string value1 = "value1";
  TxnId txn_id = 12345;
  block->AddEntry(key1, value1, txn_id, db::ValueType::PUT);

  key1 = "apply";
  value1 = "success";
  txn_id = 9876;
  block->AddEntry(key1, value1, txn_id, db::ValueType::PUT);

  key1 = "colossus";
  value1 = "thunder";
  txn_id = std::pow(2, 32) - 1;
  block->AddEntry(key1, value1, txn_id, db::ValueType::PUT);

  block->EncodeExtraInfo();

  EXPECT_TRUE(std::ranges::equal(block->GetDataView(), data_encoded));
  EXPECT_TRUE(std::ranges::equal(block->GetOffsetView(), offset_encoded));
  EXPECT_TRUE(std::ranges::equal(block->GetExtraView(), extra_encoded));
}

TEST(BlockTest, EdgeCasesEncode) {
  std::vector<Byte> data_encoded = {
      // Entry 1
      0,                        // value_type = 0(not_deleted)
      0, 0, 0, 0,               // key_len = 0
                                // key is empty
      0, 0, 0, 0,               // value_len = 0
                                // value is empty
      0xA, 0, 0, 0, 0, 0, 0, 0, // transaction_id = 12345
  };

  std::vector<Byte> offset_encoded = {
      // Offset entry 0
      0,    0, 0, 0, 0, 0, 0, 0, // start_offset of data entry 0
      0x11, 0, 0, 0, 0, 0, 0, 0, // length of data entry 0
  };

  std::vector<Byte> extra_encoded = {
      // Total entries
      0x01,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // Stating offset of offset section
      0x11,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
  };

  auto block = std::make_unique<sstable::BlockBuilder>();

  block->AddEntry("", "", 10, db::ValueType::PUT);

  block->EncodeExtraInfo();

  EXPECT_TRUE(std::ranges::equal(block->GetDataView(), data_encoded));
  EXPECT_TRUE(std::ranges::equal(block->GetOffsetView(), offset_encoded));
  EXPECT_TRUE(std::ranges::equal(block->GetExtraView(), extra_encoded));
}

TEST(BlockTest, BlockReaderIterator) {
  auto db = std::make_unique<db::DBImpl>(true /*is_testing*/);
  db->LoadDB("test");

  const db::Config *const config = db->GetConfig();
  // That number of key/value pairs is enough to create a new sst
  const int nums_elem = 10000000;

  // When db is first loaded, number of older version = 0
  EXPECT_EQ(db->GetVersionManager()->GetVersions().size(), 0);

  std::string key, value;
  size_t current_size = 0;
  int immutable_memtables_in_mem = 0;
  int last_key_index = 0;

  for (int i = 0; i < nums_elem; i++) {
    key = "key" + std::to_string(i);
    value = "value" + std::to_string(i);

    db->Put(key, value, 0 /*txn_id*/);

    current_size += key.size() + value.size();
    if (current_size >= config->GetPerMemTableSizeLimit()) {
      // Create only 1 SST
      last_key_index = i;
      break;
    }
  }

  // Force flushing immutable memtable to disk
  db->ForceFlushMemTable();

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
      EXPECT_EQ(sst_metadata[level].size(), 1);
    } else {
      EXPECT_EQ(sst_metadata[level].size(), 0);
    }
  }

  std::string filename = db->GetDBPath() + std::to_string(1) + ".sst";

  std::unique_ptr<sstable::TableReader> table_reader =
      sstable::CreateAndSetupDataForTableReader(
          std::move(filename), 1 /*sst_id*/, sst_metadata[0][0]->file_size);

  const std::vector<sstable::BlockIndex> &block_index =
      table_reader->GetBlockIndex();

  for (int i = 0; i < block_index.size(); i++) {
    uint64_t block_offset = block_index[i].GetBlockStartOffset();
    uint64_t block_size = block_index[i].GetBlockSize();

    // With each block in table, create a separate blockreader and correponding
    // blockreaderiterator
    std::unique_ptr<sstable::BlockReader> block_reader =
        table_reader->CreateAndSetupDataForBlockReader(block_offset,
                                                       block_size);
    auto iterator =
        std::make_unique<sstable::BlockReaderIterator>(block_reader.get());

    std::string prev_key, prev_value;

    // Loop forward
    for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
      std::string_view key_found = iterator->GetKey();
      std::string_view value_found = iterator->GetValue();
      db::ValueType type = iterator->GetType();
      TxnId txn_id = iterator->GetTransactionId();
      prev_key = std::string(key_found);

      EXPECT_EQ(value_found, db->Get(key_found, 0 /*txn_id*/));
      EXPECT_EQ(type, db::ValueType::PUT);
      // EXPECT_EQ(txn_id, 0);
      EXPECT_TRUE(prev_key <= key_found);

      // Test Seek(std::string_view key)
      iterator->Seek(prev_key);
      EXPECT_TRUE(iterator->IsValid());
      EXPECT_TRUE(iterator->GetKey() == prev_key);
      EXPECT_TRUE(iterator->GetValue() == value_found);
    }

    // Loop backward
    for (iterator->SeekToLast(); iterator->IsValid(); iterator->Prev()) {
      std::string_view key_found = iterator->GetKey();
      std::string_view value_found = iterator->GetValue();
      db::ValueType type = iterator->GetType();
      TxnId txn_id = iterator->GetTransactionId();
      prev_key = std::string(key_found);

      EXPECT_EQ(value_found, db->Get(key_found, 0 /*txn_id*/));
      EXPECT_EQ(type, db::ValueType::PUT);
      // EXPECT_EQ(txn_id, 0);
      EXPECT_TRUE(prev_key >= key_found);
    }
  }

  ClearAllSstFiles(db.get());
}

} // namespace kvs