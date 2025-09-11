#ifndef SSTABLE_BLOCK_H
#define SSTABLE_BLOCK_H

#include "common/macros.h"
#include "db/status.h"

// libC++
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace kvs {

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

Extra format(unit: Byte)
--------------------------------------------------------------------------
|               Extra              |                                     |
--------------------------------------------------------------------------
| total number of data entries(8B) | Start offset of Offset Section (8B) |
--------------------------------------------------------------------------
*/

namespace sstable {

class Block {
public:
  Block();

  ~Block() = default;

  // No copy allowed
  Block(const Block &) = delete;
  Block &operator=(Block &) = delete;

  // Move constructor/assignment
  Block(Block &&) = default;
  Block &operator=(Block &&) = default;

  void AddEntry(std::string_view key, std::string_view value, TxnId txn_id,
                db::ValueType value_type);

  size_t GetBlockSize() const;

  uint64_t GetNumEntries() const;

  // ONLY call this method after finish writing all data to block.
  // Otherwise, it can cause dangling pointer.
  std::span<const Byte> GetDataView();

  // ONLY call this method after finish writing all data to block.
  // Otherwise, it can cause dangling pointer.
  std::span<const Byte> GetOffsetView();

  // ONLY call this method after finish writing all data to block.
  // Otherwise, it can cause dangling pointer.
  std::span<const Byte> GetExtraView();

  void EncodeExtraInfo();

  void SearchKey(std::string_view key, TxnId txn_id);

  // Finish building the block
  void Finish();

  // Clear data of block to reuse
  void Reset();

private:
  void EncodeDataEntry(std::string_view key, std::string_view value,
                       TxnId txn_id, db::ValueType value_type);

  void EncodeOffsetEntry(size_t start_entry_offset, size_t data_entry_size);

  bool is_finished_;

  size_t block_size_;

  uint64_t num_entries_;

  // TODO(namnh) : Is there any way to avoid copying?
  std::vector<Byte> data_buffer_;

  std::vector<Byte> offset_buffer_;

  std::vector<Byte> extra_buffer_;

  // Track current offset of data section format
  uint64_t data_current_offset_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_BLOCK_H