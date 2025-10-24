#ifndef DB_LSM_H
#define DB_LSM_H

#include "common/macros.h"
#include "db/version.h"

// libC++
#include <algorithm>
#include <cassert>
#include <cmath>
#include <condition_variable>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <queue>
#include <ranges>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

// posix lib
#include <cstdio>

namespace kvs {

class ThreadPool;

namespace mvcc {
class TransactionManager;
} // namespace mvcc

namespace io {
class AppendOnlyFile;
} // namespace io

namespace sstable {
class TableBuilder;
class TableReaderCache;
class BlockReaderCache;
} // namespace sstable

namespace db {

class BaseIterator;
class BaseMemTable;
class Config;
class VersionManager;

class DBImpl {
public:
  explicit DBImpl(bool is_testing = false);

  ~DBImpl();

  // No copy allowed
  DBImpl(const DBImpl &) = delete;
  DBImpl &operator=(const DBImpl &) = delete;

  // No move allowed
  DBImpl(DBImpl &&) = default;
  DBImpl &operator=(DBImpl &&) = default;

  GetStatus Get(std::string_view key, TxnId txn_id = 0);

  void Put(std::string_view key, std::string_view value, TxnId txn_id = 0);

  void Delete(std::string_view key, TxnId txn_id = 0);

  uint64_t GetNextSSTId();

  bool LoadDB(std::string_view dbname);

  void ForceFlushMemTable();

  void AddChangesToManifest(const VersionEdit *version_edit);

  const Config *GetConfig() const;

  const VersionManager *GetVersionManager() const;

  // const sstable::BlockReaderCache *GetBlockReaderCache() const;
  const std::vector<std::unique_ptr<sstable::BlockReaderCache>> &
  GetBlockReaderCache() const;

  const sstable::TableReaderCache *GetTableReaderCache() const;

  std::string GetDBPath() const;

  void CleanupTrashFiles();

  void WakeupBgThreadToCleanupFiles(std::string_view filename) const;

  friend class Compact;

  // For testing
  const BaseMemTable *GetCurrentMemtable();

  const std::vector<std::unique_ptr<BaseMemTable>> &GetImmutableMemTables();

private:
  void Put_(std::string_view key, std::string_view value, TxnId txn_id);

  std::unique_ptr<VersionEdit> Recover(std::string_view manifest_path);

  void FlushMemTableJob(uint64_t version, int num_flush_memtables);

  void CreateNewSST(const std::unique_ptr<BaseMemTable> &immutable_memtable,
                    VersionEdit *version_edit, std::latch &work_done);

  // void AddChangesToManifest(const VersionEdit *version_edit);

  void MaybeScheduleCompaction();

  void ExecuteBackgroundCompaction();

  struct PairHash {
    std::size_t
    operator()(const std::pair<uint64_t, uint8_t> &p) const noexcept {
      // Shift the small 8-bit value into the lower bits
      return std::hash<uint64_t>()((p.first << 8) | p.second);
    }
  };

  std::string db_path_;

  std::atomic<uint64_t> next_sstable_id_;

  std::atomic<uint64_t> memtable_version_;

  // TODO(namnh)
  // An increasing monotonic number assigned to each put/delete operation.
  // it will be used until transaction module is supported
  std::atomic<uint64_t> sequence_number_;

  std::unique_ptr<BaseMemTable> memtable_;

  std::vector<std::unique_ptr<BaseMemTable>> immutable_memtables_;

  std::vector<const BaseMemTable *> flushing_memtables_;

  std::unique_ptr<mvcc::TransactionManager> txn_manager_;

  std::unique_ptr<Config> config_;

  std::atomic<bool> background_compaction_scheduled_;

  // Threadppol ISN'T COPYABLE AND MOVEABLE
  // So, we must allocate/deallocate by ourselves
  // kvs::ThreadPool *thread_pool_;
  std::unique_ptr<kvs::ThreadPool> thread_pool_;

  std::unique_ptr<sstable::TableReaderCache> table_reader_cache_;

  // std::unique_ptr<sstable::BlockReaderCache> block_reader_cache_;

  std::unique_ptr<kvs::ThreadPool> block_cache_thread_pool_;

  std::vector<std::unique_ptr<sstable::BlockReaderCache>> block_reader_cache_;

  std::unique_ptr<VersionManager> version_manager_;

  std::unique_ptr<io::AppendOnlyFile> manifest_write_object_;

  // Mutex to protect some critical data structures
  // (immutable_memtables_ list, levels_sst_info_)
  // std::shared_mutex immutable_memtables_mutex_;
  std::shared_mutex mutex_;

  std::condition_variable_any cv_;

  mutable std::queue<std::string> trash_files_;

  mutable std::mutex trash_files_mutex_;

  mutable std::condition_variable trash_files_cv_;

  std::atomic<bool> shutdown_{false};
};

} // namespace db

} // namespace kvs

#endif // DB_LSM_H