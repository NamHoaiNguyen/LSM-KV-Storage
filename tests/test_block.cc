#include <gtest/gtest.h>

#include "common/macros.h"
#include "io/linux_file.h"
#include "sstable/block.h"
#include "sstable/block_index.h"
#include "sstable/table.h"

#include <cmath>
#include <memory>

namespace kvs {

TEST(BlockTest, BasicEncode) {
  std::vector<Byte> data_encoded = {
      // Data section
      // Entry 1
      5, 0, 0, 0,                   // key_len = 5
      'a', 'p', 'p', 'l', 'e',      // key
      6, 0, 0, 0,                   // value_len = 6
      'v', 'a', 'l', 'u', 'e', '1', // value
      0x39, 0x30, 0, 0, 0, 0, 0, 0, // transaction_id = 12345
      // Entry 2
      5, 0, 0, 0,                        // key_len = 5
      'a', 'p', 'p', 'l', 'y',           // key
      7, 0, 0, 0,                        // value_len = 7
      's', 'u', 'c', 'c', 'e', 's', 's', // value
      0x94, 0x26, 0, 0, 0, 0, 0, 0,      // transaction_id = 9876
      // // Entry 3
      8, 0, 0, 0,                             // key_len = 8
      'c', 'o', 'l', 'o', 's', 's', 'u', 's', // key
      7, 0, 0, 0,                             // value_len = 7
      't', 'h', 'u', 'n', 'd', 'e', 'r',      // value
      0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0      // transaction_id = 2^32-1
  };

  std::vector<Byte> offset_encoded = {
      // Offset entry 0
      0, 0, 0, 0, 0, 0, 0, 0,    // start_offset of data entry 0
      0x1B, 0, 0, 0, 0, 0, 0, 0, // length of data entry 0
      // Offset entry 1
      0x1B, 0, 0, 0, 0, 0, 0, 0, // start_offset of data entry 1
      0x1C, 0, 0, 0, 0, 0, 0, 0, // length of data entry 1
      // Offset entry 2
      0x37, 0, 0, 0, 0, 0, 0, 0, // start_offset of data entry 2
      0x1F, 0, 0, 0, 0, 0, 0, 0, // length of data entry 2
  };

  auto block = std::make_unique<Block>();

  std::string key1 = "apple";
  std::string value1 = "value1";
  TxnId txn_id = 12345;
  block->AddEntry(key1, value1, txn_id);

  key1 = "apply";
  value1 = "success";
  txn_id = 9876;
  block->AddEntry(key1, value1, txn_id);

  key1 = "colossus";
  value1 = "thunder";
  txn_id = std::pow(2, 32) - 1;
  block->AddEntry(key1, value1, txn_id);
  EXPECT_TRUE(std::ranges::equal(block->GetDataView(), data_encoded));
  EXPECT_TRUE(std::ranges::equal(block->GetOffsetView(), offset_encoded));
}

TEST(BlockTest, EdgeCasesEncode) {
  std::vector<Byte> data_encoded = {
      // Entry 1
      0, 0, 0, 0,               // key_len = 0
                                // key is empty
      0, 0, 0, 0,               // value_len = 0
                                // value is empty
      0xA, 0, 0, 0, 0, 0, 0, 0, // transaction_id = 12345
  };

  std::vector<Byte> offset_encoded = {
      // Offset entry 0
      0,    0, 0, 0, 0, 0, 0, 0, // start_offset of data entry 0
      0x10, 0, 0, 0, 0, 0, 0, 0, // length of data entry 0
  };

  auto block = std::make_unique<Block>();

  block->AddEntry("", "", 10);
  EXPECT_TRUE(std::ranges::equal(block->GetDataView(), data_encoded));
  EXPECT_TRUE(std::ranges::equal(block->GetOffsetView(), offset_encoded));
}

} // namespace kvs