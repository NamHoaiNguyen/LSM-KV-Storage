#ifndef SSTABLE_TABLE_BUILDER_H
#define SSTABLE_TABLE_BUILDER_H

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
--------------------------------------------------
| MetaEntry | ... | MetaEntry | num_entries (8B) |
--------------------------------------------------

Meta MetaEntry format(block_meta)
-----------------------------------------------------------------------------
| 1st_key_len(4B) | 1st_key | last_key_len(4B) | last_key |                   |
| starting offset of corresponding data block (8B) | size of data block(8B) |
-----------------------------------------------------------------------------
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
  void AddEntry(std::string_view key, std::string_view value, TxnId txn_id);

  void Finish();

  // Flush block data to disk
  void FlushBlock();

  void WriteBlock();

  friend class BlockBuilderTest_Encode_Test;

private:
  std::unique_ptr<AccessFile> file_object_;

  // TODO(namnh) : unique_ptr or shared_ptr?
  std::shared_ptr<BlockBuilder> block_data_;

  size_t block_data_size_;

  // TODO(namnh) : unique_ptr?
  std::shared_ptr<BlockBuilder> block_index_;

  size_t block_index_size_;

  // TODO(namnh) : unique_ptr or shared_ptr?
  // std::unique_ptr<BaseIterator> iterator_;

  uint64_t current_offset_;
};

} // namespace kvs

#endif // SSTABLE_TABLE_BUILDER_H