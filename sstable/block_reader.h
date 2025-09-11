#ifndef SSTABLE_BLOCK_READAER_H
#define SSTABLE_BLOCK_READAER_H

#include "common/macros.h"
#include "db/status.h"

// libC++
#include <memory>
#include <span>
#include <string_view>
#include <vector>

namespace kvs {

namespace io {
class ReadOnlyFile;
}

namespace sstable {

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
  BlockReader(std::string_view filename,
              std::shared_ptr<io::ReadOnlyFile> read_file_object);
  ~BlockReader() = default;

  // No copy allowed
  BlockReader(const BlockReader &) = delete;
  BlockReader &operator=(BlockReader &) = delete;

  // Move constructor/assignment
  BlockReader(BlockReader &&) = default;
  BlockReader &operator=(BlockReader &&) = default;

  db::GetStatus SearchKey(uint64_t offset, uint64_t size, std::string_view key,
                          TxnId txn_id);

private:
  // Get starting offset of data entry at index entry_index(th) base on
  // offset_section(starting offset of offset section)
  // See block data format above
  uint64_t GetDataEntryOffset(std::span<const Byte> buffer,
                              const uint64_t offset_section,
                              const int entry_index);

  // Get type of key base on data_entry_offset(starting offset of data entry)
  // See block data format above
  db::ValueType GetValueTypeFromDataEntry(std::span<const Byte> buffer_view,
                                          uint64_t data_entry_offset);

  // Get key and value of data entry that start at data_entry_offset
  // See block data format above
  std::pair<std::string_view, std::optional<std::string_view>>
  GetKeyValueFromDataEntry(std::span<const Byte> buffer_view,
                           uint64_t data_entry_offset,
                           db::ValueType value_type);

  std::vector<Byte> buffer_;

  std::shared_ptr<io::ReadOnlyFile> read_file_object_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_READAER_H