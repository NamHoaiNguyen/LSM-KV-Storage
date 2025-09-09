#ifndef SSTABLE_TABLE_H
#define SSTABLE_TABLE_H

#include "common/macros.h"
#include "db/status.h"

// libC++
#include <memory>
#include <string>
#include <string_view>

namespace kvs {

namespace db {
class AccessFile;
class BaseIterator;
class Compact;
class Config;
} // namespace db

class SSTTest_BasicEncode_Test;

namespace io {
class ReadOnlyFile;
class WriteOnlyFile;
} // namespace io

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

namespace sstable {

class Block;
class BlockIndex;

// A SST is immutable after be written. More than that, with support of version
// that work like a snapshot of all SST files at the time an operation is
// executed, mutex is not needed. Because a SST is only visible after writing to
// disk finishes and only latest version sees this visibility
class Table {
public:
  Table(std::string &&filename, uint64_t table_id, const db::Config *config);

  ~Table() = default;

  // Copy constructor/assignment
  Table(const Table &) = default;
  Table &operator=(Table &) = default;

  // Move constructor/assignment
  Table(Table &&) = default;
  Table &operator=(Table &&) = default;

  // Add new key/value pairs to SST
  // Entries MUST be sorted in ascending order before be added
  void AddEntry(std::string_view key, std::string_view value, TxnId txn_id,
                db::ValueType value_type);

  // Flush block data to disk
  void FlushBlock();

  // After this method is executed, SST file is immutable
  void Finish();

  bool Open();

  void Read();

  db::GetStatus SearchKey(std::string_view key, TxnId txn_id);

  // Not best practise. But because table is immutable after be written, it is
  // ok
  std::string_view GetSmallestKey() const;

  // Not best practise. But because table is immutable after be written, it is
  // ok
  std::string_view GetLargestKey() const;

  uint64_t GetTableId() const;

  // For testing
  Block *GetBlockData();

  io::WriteOnlyFile *GetWriteOnlyFileObject();

private:
  void EncodeExtraInfo();

  void AddIndexBlockEntry(std::string_view first_key, std::string_view last_key,
                          uint64_t block_start_offset, uint64_t block_length);

  std::string filename_;

  uint64_t table_id_;

  std::shared_ptr<io::ReadOnlyFile> read_file_object_;

  std::unique_ptr<io::WriteOnlyFile> write_file_object_;

  // TODO(namnh) : unique_ptr or shared_ptr?
  std::unique_ptr<Block> block_data_;

  // Smallest key of each block
  std::string block_smallest_key_;

  // Largest key of each block
  std::string block_largest_key_;

  uint64_t current_offset_;

  std::vector<BlockIndex> block_index_;

  std::vector<Byte> index_block_buffer_;

  std::vector<Byte> extra_buffer_;

  // Min transaction id of block
  TxnId min_txnid_;

  // Max transaction id of block
  TxnId max_txnid_;

  // uint64_t

  std::string table_smallest_key_;

  std::string table_largest_key_;

  const db::Config *config_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_H