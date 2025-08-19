#ifndef SSTABLE_TABLE__H
#define SSTABLE_TABLE__H

#include "common/macros.h"

// libC++
#include <memory>

namespace kvs {

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

class AccessFile;
class Memtable;

// SSTable are immutable and persistent
class Table {
public:
  Table() = default;

  ~Table() = default;

  // No copy allowed
  Table(const Table &) = delete;
  Table &operator=(Table &) = delete;

  // Move constructor/assignment
  Table(Table &&) = default;
  Table &operator=(Table &&) = default;

  void Decode();

  void Encode();

private:
};

} // namespace kvs

#endif // SSTABLE_TABLE__H