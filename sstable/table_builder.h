#ifndef SSTABLE_TABLE_BUIDLER_H
#define SSTABLE_TABLE_BUIDLER_H

#include "common/macros.h"

// libC++
#include <memory>
#include <string_view>

namespace kvs {

class AccessFile;
class BlockBuilder;
class BaseIterator;

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

class TableBuilder {
public:
  TableBuilder(std::string &&filename);

  ~TableBuilder() = default;

  // Copy constructor/assignment
  TableBuilder(const TableBuilder &) = default;
  TableBuilder &operator=(TableBuilder &) = default;

  // Move constructor/assignment
  TableBuilder(TableBuilder &&) = default;
  TableBuilder &operator=(TableBuilder &&) = default;

  // Add new key/value pairs to SST
  void Add(std::string_view key, std::string_view value, TxnId txn_id);

  void Finish();

  // Flush block data to disk
  void FlushBlock();

  void WriteBlock();

private:
  std::unique_ptr<AccessFile> file_object_;

  // TODO(namnh) : unique_ptr?
  std::shared_ptr<BlockBuilder> data_block_;

  // TODO(namnh) : unique_ptr?
  std::shared_ptr<BlockBuilder> index_block_;

  // TODO(namnh) : unique_ptr or shared_ptr?
  // std::unique_ptr<BaseIterator> iterator_;
};

} // namespace kvs

#endif // SSTABLE_TABLE_BUIDLER_H