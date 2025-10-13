#include "db/db_impl.h"

#include "common/base_iterator.h"
#include "common/macros.h"
#include "common/thread_pool.h"
#include "db/compact.h"
#include "db/config.h"
#include "db/memtable.h"
#include "db/memtable_iterator.h"
#include "db/skiplist.h"
#include "db/skiplist_iterator.h"
#include "db/status.h"
#include "db/version.h"
#include "db/version_edit.h"
#include "db/version_manager.h"
#include "io/base_file.h"
#include "io/linux_file.h"
#include "mvcc/transaction.h"
#include "mvcc/transaction_manager.h"
#include "rapidjson/document.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "sstable/block_builder.h"
#include "sstable/block_reader.h"
#include "sstable/block_reader_cache.h"
#include "sstable/lru_block_item.h"
#include "sstable/lru_table_item.h"
#include "sstable/table_builder.h"
#include "sstable/table_reader.h"
#include "sstable/table_reader_cache.h"

// libC++
#include <algorithm>
#include <cassert>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <ranges>

namespace fs = std::filesystem;

namespace {
constexpr std::string kManifestFileName = "MANIFEST";

constexpr int kDefaultParseManifestBufferSize = 8192; // 8KB buffer
} // namespace

namespace kvs {

namespace db {

DBImpl::DBImpl(bool is_testing)
    : next_sstable_id_(1), memtable_(std::make_unique<MemTable>()),
      txn_manager_(std::make_unique<mvcc::TransactionManager>(this)),
      config_(std::make_unique<Config>(is_testing)),
      background_compaction_scheduled_(false),
      thread_pool_(new kvs::ThreadPool()),
      table_reader_cache_(
          std::make_unique<sstable::TableReaderCache>(this, thread_pool_)),
      block_reader_cache_(
          std::make_unique<sstable::BlockReaderCache>(thread_pool_)),
      version_manager_(std::make_unique<VersionManager>(this, thread_pool_)) {
  thread_pool_->Enqueue(&DBImpl::CleanupTrashFiles, this);
}

DBImpl::~DBImpl() {
  block_reader_cache_.reset();
  table_reader_cache_.reset();

  shutdown_ = true;
  trash_files_cv_.notify_one();

  delete thread_pool_;
  thread_pool_ = nullptr;
}

bool DBImpl::LoadDB(std::string_view dbname) {
  // Load config
  config_->LoadConfig();

  db_path_ = config_->GetSavedDataPath() + std::string(dbname) + "/";
  fs::path db_path_fs(db_path_);
  if (!fs::exists(db_path_fs)) {
    if (!fs::create_directory(db_path_fs)) {
      return false;
    }
  }

  // Load MANIFEST
  std::string manifest_path = db_path_ + kManifestFileName;
  manifest_write_object_ =
      std::make_unique<io::LinuxAppendOnlyFile>(manifest_path);
  if (!manifest_write_object_->Open()) {
    return false;
  }

  // Recover
  std::unique_ptr<VersionEdit> version_edit = Recover(manifest_path);
  if (!version_edit) {
    return false;
  }

  // Apply version edit to create new version
  version_manager_->ApplyNewChanges(std::move(version_edit));

  return true;
}

std::unique_ptr<VersionEdit> DBImpl::Recover(std::string_view manifest_path) {
  if (manifest_path.empty()) {
    return nullptr;
  }

  FILE *fp = fopen(manifest_path.data(), "r");
  if (!fp) {
    return nullptr;
  }

  char buffer[kDefaultParseManifestBufferSize];
  rapidjson::FileReadStream is(fp, buffer, sizeof(buffer));
  rapidjson::Document doc;

  auto version_edit = std::make_unique<VersionEdit>(config_->GetSSTNumLvels());
  uint64_t table_id = 0, file_size = 0;
  int level = 0;
  std::string smallest_key, largest_key, filename;

  // In MANIFEST file, there are many SST files that was added before then are
  // deleted when needing no more. These files, therefore, shouldn't be added in
  // list to recheck when parsing "delete_files". When meeting a table id in
  // delete_files, and that table id also is in this map, remove it from this
  // map. So this table won't be added into VersionEdit.
  std::unordered_map<SSTId, std::shared_ptr<SSTMetadata>> filter_add_files;

  while (true) {
    doc.ParseStream<rapidjson::kParseStopWhenDoneFlag>(is);
    if (doc.HasParseError()) {
      if (feof(fp)) {
        break; // reached EOF cleanly
      }
      return nullptr;
    }

    // Parse next_table_id
    if (doc.HasMember("next_table_id") && doc["next_table_id"].IsInt64()) {
      // Update next id used for new table
      if (doc["next_table_id"].GetInt64() > next_sstable_id_) {
        next_sstable_id_ = doc["next_table_id"].GetInt64();
      }
    }

    // Parse sequence_number
    if (doc.HasMember("sequence_number") && doc["sequence_number"].IsInt64()) {
      // Update next id used for new table
      if (doc["sequence_number"].GetInt64() > sequence_number_) {
        sequence_number_ = doc["sequence_number"].GetInt64();
      }
    }

    // Parse new_files array
    if (doc.HasMember("new_files") && doc["new_files"].IsArray()) {
      for (auto &file : doc["new_files"].GetArray()) {
        table_id = file["id"].GetInt64();
        level = file["level"].GetInt();
        file_size = file["size"].GetInt64();
        smallest_key = file["smallest_key"].GetString();
        largest_key = file["largest_key"].GetString();
        // Build filename
        filename = db_path_ + std::to_string(table_id) + ".sst";

        auto sst_metadata = std::make_shared<SSTMetadata>(
            table_id, level, file_size, smallest_key, largest_key,
            std::move(filename));
        filter_add_files.insert({table_id, sst_metadata});
      }
    }

    // Parse delete_files array
    if (doc.HasMember("delete_files") && doc["delete_files"].IsArray()) {
      for (auto &file : doc["delete_files"].GetArray()) {
        table_id = file["id"].GetInt64();
        level = file["level"].GetInt();

        auto iterator = filter_add_files.find(table_id);
        if (iterator != filter_add_files.end()) {
          // If found in map
          filter_add_files.erase(iterator);
        }

        version_edit->RemoveFiles(table_id, level);
      }
    }
  }

  // Close
  fclose(fp);

  // Push all data from filter_add_files into VersionEdit
  for (const auto &sst_metadata : filter_add_files) {
    version_edit->AddNewFiles(sst_metadata.second);
  }

  // Clear map when finishing
  filter_add_files.clear();

  return version_edit;
}

void DBImpl::CleanupTrashFiles() {
  while (!shutdown_) {
    {
      std::unique_lock rwlock(trash_files_mutex_);
      trash_files_cv_.wait(rwlock, [this]() {
        return this->shutdown_ || !this->trash_files_.empty();
      });

      if (this->shutdown_) {
        return;
      }

      while (!trash_files_.empty()) {
        std::string filename = trash_files_.front();
        trash_files_.pop();

        fs::path file_path(filename);
        if (fs::exists(file_path) && fs::is_regular_file(file_path)) {
          fs::remove(file_path);
        }
      }
    }
  }
}

void DBImpl::WakeupBgThreadToCleanupFiles(std::string_view filename) const {
  // std::string file_name = std::string(filename);

  // std::scoped_lock rwlock(trash_files_mutex_);
  // trash_files_.push(std::move(file_name));
  // trash_files_cv_.notify_one();
}

GetStatus DBImpl::Get(std::string_view key, TxnId txn_id) {
  GetStatus status;

  {
    std::shared_lock rlock(mutex_);

    // Find data from Memtable
    status = memtable_->Get(key, txn_id);
    if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
      return status;
    }

    // If key is not found, continue finding from immutable memtables
    for (const auto &immu_memtable :
         immutable_memtables_ | std::views::reverse) {
      status = immu_memtable->Get(key, txn_id);
      if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
        return status;
      }
    }
  }

  // TODO(nanmh) : Does it need to acquire lock when looking up key in SSTs?
  const Version *version = version_manager_->GetLatestVersion();
  if (!version) {
    // std::cout << "namnh check value of type " << status.type << std::endl;
    return status;
  }

  version->IncreaseRefCount();
  status = version->Get(key, txn_id);
  // if (status.type == ValueType::PUT || status.type == ValueType::DELETED) {
  //   return status;
  // }

  version->DecreaseRefCount();

  // if (status.type == ValueType::NOT_FOUND) {
  //   std::cout << "namnh check value of type 2" << status.type << std::endl;
  // }

  return status;
}

void DBImpl::Put(std::string_view key, std::string_view value, TxnId txn_id) {
  // TODO(namnh) : // Change when transaction is supported
  sequence_number_++;
  Put_(key, value, sequence_number_.load());

  // Put_(key, value, txn_id);
}

void DBImpl::Delete(std::string_view key, TxnId txn_id) {
  // TODO(namnh) : // Change when transaction is supported
  sequence_number_++;
  Put_(key, std::string_view{}, sequence_number_.load());

  // Put_(key, std::string_view{}, txn_id);
}

void DBImpl::Put_(std::string_view key, std::string_view value, TxnId txn_id) {
  std::unique_lock rwlock(mutex_);
  // Stop writing when numbers of immutable memtable reach to threshold
  // TODO(namnh): Write Stop problem. Improve in future
  cv_.wait(rwlock, [this]() {
    return immutable_memtables_.size() < config_->GetMaxImmuMemTablesInMem();
  });

  if (value == std::string_view{}) {
    memtable_->Delete(key, txn_id);
  } else {
    memtable_->Put(key, value, txn_id);
  }

  if (memtable_->GetMemTableSize() >= config_->GetPerMemTableSizeLimit()) {
    immutable_memtables_.push_back(std::move(memtable_));

    if (immutable_memtables_.size() >= config_->GetMaxImmuMemTablesInMem()) {
      // Flush thread to flush memtable to disk
      thread_pool_->Enqueue(&DBImpl::FlushMemTableJob, this);
    }

    // Create new empty mutable memtable
    memtable_ = std::make_unique<MemTable>();
  }
}

// Just for testing
void DBImpl::ForceFlushMemTable() {
  {
    std::scoped_lock lock(mutex_);
    if (memtable_->GetMemTableSize() != 0) {
      immutable_memtables_.push_back(std::move(memtable_));
      // Create new empty mutable memtable
      memtable_ = std::make_unique<MemTable>();
    }
  }

  FlushMemTableJob();
}

void DBImpl::FlushMemTableJob() {
  // Create new SSTs
  std::latch all_done(immutable_memtables_.size());

  std::vector<std::shared_ptr<SSTMetadata>> new_ssts_info;
  auto version_edit = std::make_unique<VersionEdit>(config_->GetSSTNumLvels());

  {
    std::scoped_lock rwlock(mutex_);
    for (const auto &immutable_memtable : immutable_memtables_) {
      thread_pool_->Enqueue(&DBImpl::CreateNewSST, this,
                            std::cref(immutable_memtable), version_edit.get(),
                            std::ref(all_done));
    }
  }

  // Wait until all workers have finished
  all_done.wait();

  version_edit->SetNextTableId(GetNextSSTId());
  version_edit->SetSequenceNumber(sequence_number_);

  // Apply versionEdit to manifest and fsync to persist data
  AddChangesToManifest(version_edit.get());

  // Not until this point that latest version is visible
  version_manager_->ApplyNewChanges(std::move(version_edit));

  // Notify to let writing continues.
  // NOTE: new writes are only allowed after new version is VISIBLE
  {
    std::scoped_lock rwlock(mutex_);
    immutable_memtables_.clear();
    cv_.notify_all();
  }

  MaybeScheduleCompaction();
}

void DBImpl::CreateNewSST(
    const std::unique_ptr<BaseMemTable> &immutable_memtable,
    VersionEdit *version_edit, std::latch &work_done) {
  assert(version_edit);

  uint64_t sst_id = GetNextSSTId();
  std::string filename = db_path_ + std::to_string(sst_id) + ".sst";
  sstable::TableBuilder new_sst(std::move(filename), config_.get());

  if (!new_sst.Open()) {
    work_done.count_down();
    return;
  }

  auto iterator = std::make_unique<MemTableIterator>(immutable_memtable.get());
  // Iterate through all key/value pairs to add them to sst
  for (iterator->SeekToFirst(); iterator->IsValid(); iterator->Next()) {
    new_sst.AddEntry(iterator->GetKey(), iterator->GetValue(),
                     iterator->GetTransactionId(), iterator->GetType());
  }

  // All of key/value pairs had been written to sst. SST should be flushed to
  // disk
  new_sst.Finish();

  {
    std::scoped_lock lock(mutex_);
    std::string filename = db_path_ + std::to_string(sst_id) + ".sst";
    version_edit->AddNewFiles(sst_id, 0 /*level*/, new_sst.GetFileSize(),
                              new_sst.GetSmallestKey(), new_sst.GetLargestKey(),
                              std::move(filename));
  }

  // Signal that this worker is done
  work_done.count_down();
}

void DBImpl::AddChangesToManifest(const VersionEdit *version_edit) {
  rapidjson::Document doc;
  doc.SetObject();

  rapidjson::Document::AllocatorType &allocator = doc.GetAllocator();

  // Encode next table id
  doc.AddMember("next_table_id", version_edit->GetNextTableId(), allocator);

  // Encode sequence number
  doc.AddMember("sequence_number", version_edit->GetSequenceNumber(),
                allocator);

  // List of new files created
  rapidjson::Value new_files_array(rapidjson::kArrayType);
  // List of delete files
  rapidjson::Value delete_files_array(rapidjson::kArrayType);

  const std::vector<std::vector<std::shared_ptr<SSTMetadata>>> &new_files_list =
      version_edit->GetImmutableNewFiles();
  const std::set<std::pair<SSTId, int>> &delete_files_list =
      version_edit->GetImmutableDeletedFiles();

  for (int level = 0; level < new_files_list.size(); level++) {
    for (const auto &new_file : new_files_list[level]) {
      rapidjson::Value new_file_obj(rapidjson::kObjectType);

      // Encode table id
      new_file_obj.AddMember("id", new_file->table_id, allocator);

      // Encode file level
      new_file_obj.AddMember("level", new_file->level, allocator);

      // Encode file size
      new_file_obj.AddMember("size", new_file->file_size, allocator);

      // Encode smallest key
      rapidjson::Value smallest_key_value;
      smallest_key_value.SetString(
          new_file->smallest_key.data(),
          static_cast<rapidjson::SizeType>(new_file->smallest_key.size()),
          allocator);
      new_file_obj.AddMember("smallest_key", smallest_key_value, allocator);

      // Encode largest key
      rapidjson::Value largest_key_value;
      largest_key_value.SetString(
          new_file->largest_key.data(),
          static_cast<rapidjson::SizeType>(new_file->largest_key.size()),
          allocator);
      new_file_obj.AddMember("largest_key", largest_key_value, allocator);

      // Add file object to array
      new_files_array.PushBack(new_file_obj, allocator);
    }
  }

  for (const auto &delete_file : delete_files_list) {
    rapidjson::Value delete_file_obj(rapidjson::kObjectType);

    // Encode table id
    delete_file_obj.AddMember("id", delete_file.first, allocator);
    // Encode file level
    delete_file_obj.AddMember("level", delete_file.second, allocator);

    // Add file object to array
    delete_files_array.PushBack(delete_file_obj, allocator);
  }

  // Add new_files_array to root
  if (!new_files_array.Empty()) {
    doc.AddMember("new_files", new_files_array, allocator);
  }

  if (!delete_files_array.Empty()) {
    // Add delete_files_array to root
    doc.AddMember("delete_files", delete_files_array, allocator);
  }

  // Serialize to string
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);

  std::span<const Byte> bytes(
      reinterpret_cast<const uint8_t *>(buffer.GetString()), buffer.GetSize());

  // TODO(namnh, IMPORTANT) : What if append fail ?
  // TODO(namnh, IMPORTANT) : Do we need to lock file ?
  manifest_write_object_->Append(bytes);
}

void DBImpl::MaybeScheduleCompaction() {
  // std::scoped_lock rwlock(mutex_);
  if (background_compaction_scheduled_.load()) {
    // only 1 compaction happens at a moment. This condition is highest
    // privilege
    return;
  }

  if (!version_manager_->NeedSSTCompaction()) {
    return;
  }

  if (!background_compaction_scheduled_.exchange(true)) {
    thread_pool_->Enqueue(&DBImpl::ExecuteBackgroundCompaction, this);
  }
}

void DBImpl::ExecuteBackgroundCompaction() {
  const Version *version = version_manager_->GetLatestVersion();
  if (!version) {
    return;
  }

  version->IncreaseRefCount();
  auto version_edit = std::make_unique<VersionEdit>(config_->GetSSTNumLvels());
  auto compact = std::make_unique<Compact>(block_reader_cache_.get(),
                                           table_reader_cache_.get(), version,
                                           version_edit.get(), this);
  bool compact_success = compact->PickCompact();
  if (!compact_success) {
    version->DecreaseRefCount();

    background_compaction_scheduled_.store(false);
    // std::this_thread::sleep_for(std::chrono::milliseconds(150));
    MaybeScheduleCompaction();

    return;
  }

  version->DecreaseRefCount();

  version_edit->SetNextTableId(GetNextSSTId());
  version_edit->SetSequenceNumber(sequence_number_);

  // Apply versionEdit to manifest and fsync to persist data
  AddChangesToManifest(version_edit.get());

  // Apply compact version edit(changes) to create new version
  version_manager_->ApplyNewChanges(std::move(version_edit));

  // Compaction can create many files, so maybe we need another compaction
  // round
  background_compaction_scheduled_.store(false);
  MaybeScheduleCompaction();
}

uint64_t DBImpl::GetNextSSTId() { return next_sstable_id_.fetch_add(1); }

const Config *DBImpl::GetConfig() const { return config_.get(); }

const VersionManager *DBImpl::GetVersionManager() const {
  return version_manager_.get();
}

std::string DBImpl::GetDBPath() const { return db_path_; }

const BaseMemTable *DBImpl::GetCurrentMemtable() { return memtable_.get(); }

const std::vector<std::unique_ptr<BaseMemTable>> &
DBImpl::GetImmutableMemTables() {
  return immutable_memtables_;
}

const sstable::BlockReaderCache *DBImpl::GetBlockReaderCache() const {
  return block_reader_cache_.get();
}

const sstable::TableReaderCache *DBImpl::GetTableReaderCache() const {
  return table_reader_cache_.get();
}

} // namespace db

} // namespace kvs