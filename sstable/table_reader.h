#ifndef SSTABLE_TABLE_READER_H
#define SSTABLE_TABLE_READER_H

#include "common/macros.h"
#include "db/status.h"
#include "io/linux_file.h"

// libC++
#include <cassert>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace kvs {

namespace io {
class ReadOnlyFile;
}

namespace sstable {
class BlockIndex;
class BlockReaderCache;
class BlockReaderData;
class BlockReader;

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

struct TableReaderData {
  TableReaderData() = default;

  // No copy allowed
  TableReaderData(const TableReaderData &) = delete;
  TableReaderData &operator=(TableReaderData &) = delete;

  // Move constructor/assignment
  TableReaderData(TableReaderData &&) = default;
  TableReaderData &operator=(TableReaderData &&) = default;

  std::string filename;

  SSTId table_id;

  uint64_t file_size;

  TxnId min_transaction_id;

  TxnId max_transaction_id;

  std::vector<BlockIndex> block_index;

  std::unique_ptr<io::ReadOnlyFile> read_file_object;
};

// TODO(namnh) : With this design, a need for table reader cache becomes an
// indispensable requirement
class TableReader {
public:
  explicit TableReader(std::unique_ptr<TableReaderData> table_reader_data);

  ~TableReader() = default;

  // No copy allowed
  TableReader(const TableReader &) = delete;
  TableReader &operator=(TableReader &) = delete;

  // No move allowed
  TableReader(TableReader &&) = delete;
  TableReader &operator=(TableReader &&) = delete;

  db::GetStatus SearchKey(std::string_view key, TxnId txn_id,
                          const sstable::BlockReaderCache *block_reader_cache,
                          const TableReader *table_reader) const;

  std::unique_ptr<BlockReader>
  CreateAndSetupDataForBlockReader(BlockOffset offset,
                                   uint64_t block_size) const;

  uint64_t GetFileSize() const;

  friend class TableReaderIterator;

  // For testing
  const std::vector<BlockIndex> &GetBlockIndex() const;

private:
  std::pair<BlockOffset, BlockSize>
  GetBlockOffsetAndSize(std::string_view key) const;

  const std::string filename_;

  const SSTId table_id_;

  // Size of SST file
  const uint64_t file_size_;

  // Min transaction id contained in SST
  const TxnId min_transaction_id_;

  // Max transaction id contained in SST
  const TxnId max_transaction_id_;

  // Contain starting offset and size of each block in table
  // It is not const, because const object prevent moveable
  std::vector<BlockIndex> block_index_;

  std::unique_ptr<io::ReadOnlyFile> read_file_object_;
};

std::unique_ptr<TableReader>
CreateAndSetupDataForTableReader(std::string &&filename, SSTId table_id,
                                 uint64_t file_size);

void DecodeExtraInfo(TableReaderData *table_reader_data);

void FetchBlockIndexInfo(uint64_t total_block_entries,
                         uint64_t starting_meta_section_offset,
                         uint64_t meta_section_length,
                         TableReaderData *table_reader_data);

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_H