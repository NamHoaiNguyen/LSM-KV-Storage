#ifndef SSTABLE_TABLE_READER_H
#define SSTABLE_TABLE_READER_H

#include "common/macros.h"
#include "db/status.h"
#include "io/linux_file.h"

// libC++
#include <cassert>
#include <memory>
#include <string>
#include <vector>

namespace kvs {

namespace io {
class ReadOnlyFile;
}

namespace sstable {
class BlockIndex;
class BlockReaderCache;

/*
SST data format
-------------------------------------------------------------------------------
|         Block Section         |    Meta Section   |          Extra          |
-------------------------------------------------------------------------------
| data block | ... | data block |      metadata     |        Extra info       |
-------------------------------------------------------------------------------

Meta Section format
-------------------------------
| MetaEntry | ... | MetaEntry |
-------------------------------

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

// TODO(namnh) : With this design, a need for table reader cache becomes an
// indispensable requirement
class TableReader {
public:
  TableReader(std::string &&filename, SSTId table_id, uint64_t file_size);

  ~TableReader() = default;

  // No copy allowed
  TableReader(const TableReader &) = delete;
  TableReader &operator=(TableReader &) = delete;

  // Move constructor/assignment
  TableReader(TableReader &&) = default;
  TableReader &operator=(TableReader &&) = default;

  bool Open();

  db::GetStatus SearchKey(
      std::string_view key, TxnId txn_id,
      const sstable::BlockReaderCache *block_reader_cache) const;

  const std::shared_ptr<io::ReadOnlyFile> GetReadFileObject() const;

  uint64_t GetFileSize() const;

  const std::vector<BlockIndex> &GetBlockIndex() const;

private:
  void DecodeExtraInfo(uint64_t *total_block_entries,
                       uint64_t *starting_meta_section_offset,
                       uint64_t *meta_section_length);

  void FetchBlockIndexInfo(uint64_t total_block_entries,
                           uint64_t starting_meta_section_offset,
                           uint64_t meta_section_length);

  const std::string filename_;

  const SSTId table_id_;

  const uint64_t file_size_;

  TxnId min_transaction_id_;

  TxnId max_transaction_id_;

  std::vector<BlockIndex> block_index_;

  const std::shared_ptr<io::ReadOnlyFile> read_file_object_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_H