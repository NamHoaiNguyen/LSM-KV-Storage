#ifndef SSTABLE_TABLE_READER_H
#define SSTABLE_TABLE_READER_H

#include "common/macros.h"
#include "db/status.h"
#include "io/linux_file.h"

// libC++
#include <memory>
#include <string>
#include <vector>

namespace kvs {

namespace io {
class ReadOnlyFile;
}

namespace sstable {
class BlockIndex;

/*
SST data format
-------------------------------------------------------------------------------
|         Block Section         |    Meta Section   |          Extra          |
-------------------------------------------------------------------------------
| data block | ... | data block |      metadata     |        Extra info       |
-------------------------------------------------------------------------------

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
-------------------------------------------------------------------------------
| Total block entries(8B) | Meta section offset(8B) | Meta section length(8B) |
|  Min TransactionId(8B)  |  Max TransactionId(8B)  |                         |
-------------------------------------------------------------------------------
*/

class TableReader {
public:
  TableReader(std::string &&filename);

  ~TableReader() = default;

  // No copy allowed
  TableReader(const TableReader &) = delete;
  TableReader &operator=(TableReader &) = delete;

  // Move constructor/assignment
  TableReader(TableReader &&) = default;
  TableReader &operator=(TableReader &&) = default;

  bool Open();

  db::GetStatus SearchKey(std::string_view key, TxnId txn_id) const;

  // Not best practise. But because table reader is immutable, it is ok
  std::string_view GetSmallestKey() const { return smallest_key_; }

  // Not best practise. But because table reader is immutable, it is ok
  std::string_view GetLargestKey() const { return largest_key_; }

private:
  std::string filename_;

  SSTId sst_id_;

  std::string smallest_key_;

  std::string largest_key_;

  std::vector<BlockIndex> block_index_;

  std::shared_ptr<io::ReadOnlyFile> read_file_object_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_H