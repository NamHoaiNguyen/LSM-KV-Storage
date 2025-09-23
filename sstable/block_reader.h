#ifndef SSTABLE_BLOCK_READAER_H
#define SSTABLE_BLOCK_READAER_H

#include "common/macros.h"
#include "db/status.h"

// libC++
#include <cassert>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace kvs {

namespace io {
class ReadOnlyFile;
}

namespace sstable {

struct BlockReaderData {
  BlockReaderData(uint64_t block_size) {
    assert(block_size > 0);
    buffer.resize(block_size);
  }

  // Move constructor/assignment
  BlockReaderData(BlockReaderData &&) = default;

  BlockReaderData &operator=(BlockReaderData &&) = default;

  // Total data entries in a block
  uint64_t total_data_entries;

  // Starting offset of offset section
  uint64_t offset_section;

  // Contain starting offset of each data entries
  std::vector<uint64_t> data_entries_offset_info;

  // Buffer that data from block is written into
  std::vector<Byte> buffer;
};

/*
Block data format(unit: Byte)
--------------------------------------------------------------------------------
|            Data Section         |           Offset Section         |  Extra  |
--------------------------------------------------------------------------------
||Data Entry#1| ... | DataEntry#N|||Offset Entry#1|...|Offset Entry#N|   Info  |
--------------------------------------------------------------------------------


Data entry format(unit: Byte)
if ValueType = PUT
--------------------------------------------------------------------------------
|                                 Data Entry                                   |
--------------------------------------------------------------------------------
| ValueType | key_len (4B) | key | value_len (4B) | value | transaction_id(8B) |
--------------------------------------------------------------------------------

if ValueType = DELETE
-------------------------------------------------------
|                      Data Entry                     |
-------------------------------------------------------
| ValueType | key_len (4B) | key | transaction_id(8B) |
-------------------------------------------------------
(Valuetype(uint8_t) shows that value is deleted or not)
(0 = PUT = NOT DELETED)
(1 = DELETE = DELETED)
db/value_type.h


Offset entry format(unit: Byte)
-----------------------------------------------------------------------------
|                              Offset Entry                                 |
-----------------------------------------------------------------------------
| Starting offset of coresponding data entry(8B) |  Size of data entry(8B)  |
-----------------------------------------------------------------------------

Extra format
--------------------------------------------------------------------------
|               Extra              |                                     |
--------------------------------------------------------------------------
| total number of data entries(8B) | Start offset of Offset Section (8B) |
--------------------------------------------------------------------------
*/

class BlockReader {
public:
  BlockReader(std::unique_ptr<BlockReaderData> block_reader_data);
  ~BlockReader() = default;

  // No copy allowed
  BlockReader(const BlockReader &) = delete;
  BlockReader &operator=(BlockReader &) = delete;

  // Move constructor/assignment
  BlockReader(BlockReader &&) = default;
  BlockReader &operator=(BlockReader &&) = default;

  db::GetStatus SearchKey(std::string_view key, TxnId txn_id) const;

  friend class BlockReaderIterator;

private:
  // Get starting offset of data entry at index entry_index(th) base on
  // offset_section(starting offset of offset section)
  // See block data format above
  uint64_t GetDataEntryOffset(int entry_index) const;

  // Get type of key base on data_entry_offset(starting offset of data entry)
  // See block data format above
  db::ValueType GetValueTypeFromDataEntry(uint64_t data_entry_offset) const;

  // Get key of data entry that start at data_entry_offset
  std::string_view GetKeyFromDataEntry(uint64_t data_entry_offset) const;

  // Get value of data entry that start at data_entry_offset
  std::string_view GetValueFromDataEntry(uint64_t data_entry_offset) const;

  TxnId GetTransactionIdFromDataEntry(uint64_t data_entry_offset) const;

  // All of below objects are non-const to be moveable. But we need them to be
  // immutable. So, ALL of methods in this class MUST BE const to avoid these
  // data member be accidentaly updated
  std::vector<Byte> buffer_;

  // Total data entries in a block
  uint64_t total_data_entries_;

  // Starting offset of offset section
  uint64_t offset_section_;

  // Contain starting offset of each data entries
  std::vector<uint64_t> data_entries_offset_info_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_READAER_H