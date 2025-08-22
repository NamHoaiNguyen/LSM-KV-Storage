#ifndef SSTABLE_TABLE_H
#define SSTABLE_TABLE_H

#include "common/macros.h"

// libC++
#include <memory>
#include <string_view>

namespace kvs {

class AccessFile;
class Block;
class BaseIterator;
class LinuxAccessFile;

/*
SST data format
-------------------------------------------------------------------------------
|         Block Section         |    Meta Section   |          Extra          |
-------------------------------------------------------------------------------
| data block | ... | data block |      metadata     | meta block offset (u32) |

Meta Section format
---------------------------------------------------------------
| num_entries (32) | MetaEntry | ... | MetaEntry | Hash (32) |
---------------------------------------------------------------

Meta MetaEntry format
 ---------------------------------------------------------------------------------------------------
 | offset(32) | 1st_key_len(16) | 1st_key(1st_key_len) | last_key_len(16)
|last_key(last_key_len) |
 ---------------------------------------------------------------------------------------------------

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
  void AddEntry(std::string_view key, std::string_view value, TxnId txn_id);

  void Finish();

  // Flush block data to disk
  void FlushBlock();

  void WriteBlock();

  friend class BlockBuilderTest_Encode_Test;

private:
  std::unique_ptr<AccessFile> file_object_;

  // TODO(namnh) : unique_ptr or shared_ptr?
  std::shared_ptr<Block> data_block_;

  // TODO(namnh) : unique_ptr?
  std::shared_ptr<Block> index_block_;

  // TODO(namnh) : unique_ptr or shared_ptr?
  // std::unique_ptr<BaseIterator> iterator_;

  uint64_t current_offset_;
};

} // namespace kvs

#endif // SSTABLE_TABLE_H