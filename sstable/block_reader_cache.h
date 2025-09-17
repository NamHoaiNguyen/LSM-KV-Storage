#ifndef SSTABLE_TABLE_READER_CACHE_H
#define SSTABLE_TABLE_READER_CACHE_H

#include "common/macros.h"
#include "sstable/block_index.h"

// libC++
#include <cassert>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>

namespace kvs {

namespace sstable {

class BlockReader;
;
class BlockReaderCache {
public:
  explicit BlockReaderCache(const TableReaderCache* table_reader_cache);

  ~BlockReaderCache() = default;

  // No copy allowed
  BlockReaderCache(const BlockReaderCache &) = delete;
  BlockReaderCache &operator=(BlockReaderCache &) = delete;

  // Move constructor/assignment
  BlockReaderCache(BlockReaderCache &&) = default;
  BlockReaderCache &operator=(BlockReaderCache &&) = default;

  GetStatus GetKeyFromBlockCache(std::string_view key,
                                 TxnId txn_id,
                                 std::pair<SSTId, BlockOffset> block_info) const;

private:
  const TableReader *GetBlockReader(std::pair<SSTId, BlockOffset>lock_info) const;

  // Custom hash for pair<int, int>
  struct pair_hash {
    size_t operator()(const pair<SSTId, BlockOffset>& p) const noexcept {
      // Combine the hashes of both sst id and block offset
      return std::hash<uin64_t>()(p.first)
          ^ (std::hash<uin64_t>()(p.second) << 1);
    }
  };

  struct pair_equal {
    bool operator()(const pair<SSTId, BlockOffset>& a,
                    const pair<SSTId, BlockOffset>& b) const noexcept {
      return a.first == b.first && a.second == b.second;
    }
  };

  // Each key of block reader item in block reader cache is the combination
  // of table id and block offset
  std::unordered_map<std::pair<SSTId, BlockOffset>,
                     std::unique_ptr<BlockReader>,
                     pair_hash, pair_equal> block_reader_cache_;

  const TableReaderCache* table_reader_cache_;

  mutable std::shared_mutex mutex_;
};

} // namespace sstable

} // namespace kvs

#endif // SSTABLE_TABLE_READER_CACHE_H