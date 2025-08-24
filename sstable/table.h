#ifndef SSTABLE_TABLE_H
#define SSTABLE_TABLE_H

#include "common/macros.h"

// libC++
#include <memory>
#include <string>
#include <string_view>

namespace kvs {

class AccessFile;
class Block;
class BlockIndex;
class BaseIterator;
class LinuxAccessFile;

/*
SST data format
-------------------------------------------------------------------------------
|         Block Section         |    Meta Section   |          Extra          |
-------------------------------------------------------------------------------
| data block | ... | data block |      metadata     |        Extra info       |

Meta Section format
--------------------------------------------------
| MetaEntry | ... | MetaEntry | num_entries (8B) |
--------------------------------------------------

MetaEntry format(block_meta)(in order from top to bottom, left to right)
-----------------------------------------------------------------------------
| 1st_key_len(4B) | 1st_key | last_key_len(4B) | last_key |                 |
| starting offset of corresponding data block (8B) | size of data block(8B) |
-----------------------------------------------------------------------------

Extra format(in order from top to bottom, left to right)
-----------------------------------------------------
| Meta section offset(8B) | Meta section length(8B) |
|  Min TransactionId(8B)  |  Max TransactionId(8B)  |
-----------------------------------------------------
*/

class Table {
public:
  Table(std::string &&filename);

  ~Table() = default;

  // Copy constructor/assignment
  Table(const Table &) = default;
  Table &operator=(Table &) = default;

  // Move constructor/assignment
  Table(Table &&) = default;
  Table &operator=(Table &&) = default;

  // Add new key/value pairs to SST
  // Entries MUST be sorted in ascending order before be added
  void AddEntry(std::string_view key, std::string_view value, TxnId txn_id);

  // Flush block data to disk
  void FlushBlock();

  void Finish();

  bool Open();

  // For testing
  friend class TableTest_BasicEncode_Test;
  Block *GetBlockData();

  BlockIndex *GetBlockIndexData();

private:
  void EncodeExtraInfo();

  std::unique_ptr<AccessFile> file_object_;

  // TODO(namnh) : unique_ptr or shared_ptr?
  std::unique_ptr<Block> block_data_;

  std::string block_first_key_;

  std::string block_last_key_;

  // TODO(namnh) : unique_ptr?
  std::unique_ptr<BlockIndex> block_index_;

  // TODO(namnh) : unique_ptr or shared_ptr?
  // std::unique_ptr<BaseIterator> iterator_;

  uint64_t current_offset_;

  std::vector<Byte> extra_buffer_;

  // Min transaction id of block
  TxnId min_txnid_;

  // Max transaction id of block
  TxnId max_txnid_;
};

} // namespace kvs

#endif // SSTABLE_TABLE_H