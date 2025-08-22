#ifndef SSTABLE_BLOCK_BUILDER_H
#define SSTABLE_BLOCK_BUILDER_H

#include "common/macros.h"

// libC++
#include <optional>
#include <string>
#include <string_view>

namespace kvs {

// class BlockBuilderTest;

/*
Block data format
--------------------------------------------------------------------------------
|            Data Section         |           Offset Section         |  Extra  |
--------------------------------------------------------------------------------
||Data Entry#1| ... | DataEntry#N|||Offset Entry#1|...|Offset Entry#N|   Info  |
--------------------------------------------------------------------------------


Data entry format
--------------------------------------------------------------------
|                           Data Entry                             |
--------------------------------------------------------------------
| key_len (4B) | key | value_len (4B) | value | transaction_id(8B) |
--------------------------------------------------------------------

Offset entry format
-----------------------------------------------------------------------------
|                              Offset Entry                                 |
-----------------------------------------------------------------------------
| Starting offset of coresponding data entry(8B) |  Size of data entry(8B)  |
-----------------------------------------------------------------------------

Extra format
------------------------------------
|               Extra              |
------------------------------------
| total number of data entries(8B) |
------------------------------------
*/

class BlockBuilder {
public:
  BlockBuilder();

  ~BlockBuilder() = default;

  // No copy allowed
  BlockBuilder(const BlockBuilder &) = default;
  BlockBuilder &operator=(BlockBuilder &) = default;

  // Move constructor/assignment
  BlockBuilder(BlockBuilder &&) = default;
  BlockBuilder &operator=(BlockBuilder &&) = default;

  void Add(std::string_view key, std::string_view value, TxnId txn_id);

  // Finish building the block
  void Finish();

  // Clear data of block to reuse
  void Reset();

  size_t GetBlockSize() const;

  uint64_t GetNumEntries() const;

  friend class BlockBuilderTest_Encode_Test;

private:
  bool is_finished_;

  size_t block_size_;

  uint64_t num_entries_;

  // TODO(namnh) : Is there any way to avoid copying?
  std::vector<Byte> buffer_;
};

} // namespace kvs

#endif // SSTABLE_BLOCK_BUILDER_H