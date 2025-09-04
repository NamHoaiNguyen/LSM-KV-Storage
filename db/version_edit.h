// #ifndef DB_VERSION_EDIT_H
// #define DB_VERSION_EDIT_H

// #include "common/macros.h"

// #include <memory>
// #include <vector>

// namespace kvs {

// namespace sstable {
// class Table;
// } // namespace sstable

// namespace db {

// class BaseMemTable;
// class Compact;
// class Config;
// class DBImpl;

// class VersionEdit {
// public:
//   VersionEdit() = default;

//   ~VersionEdit() = default;

//   // No copy allowed
//   VersionEdit(const VersionEdit &) = delete;
//   VersionEdit &operator=(VersionEdit &) = delete;

//   // Move constructor/assignment
//   VersionEdit(VersionEdit &&) = default;
//   VersionEdit &operator=(VersionEdit &&) = default;

//   bool CreateNewSST(const BaseMemTable *const immutable_memtable);

//   friend class Compact;

// private:
//   std::vector<> deleted_files;

//   std::vector<> added_files_
// };

// } // namespace db

// } // namespace kvs

// #endif // DB_VERSION_EDIT_H