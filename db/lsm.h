#ifndef DB_LSM_H
#define DB_LSM_H

#include "common/macros.h"

#include <memory>
#include <vector>

namespace kvs {

class Memtable;
class SSTable;
class TransactionManager;

class LSM {
  public:
    LSM();

    ~LSM();

    // Copy constructor and assignment
    LSM(const LSM&) = delete;
    LSM& operator=(const LSM&) = delete;

    // Move constructor and assigment
    // TODO(namnh) : Recheck this one.
    LSM(LSM&&) = default;
    LSM& operator=(LSM&&) = default;

  private:
    // TODO(namnh) : unique_ptr or shared_ptr
    std::unique_ptr<TransactionManager> txns_manager_;

    // TODO(namnh) : unique_ptr or shared_ptr
    std::vector<std::unique_ptr<Memtable>> memtables_;

    std::vector<std::unique_ptr<Memtable>> immutable_memtables_;

    std::unique_ptr<Memtable> memtable_;

};

} // namespace kvs

#endif // DB_LSM_H