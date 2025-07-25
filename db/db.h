#ifndef DB_ENGINE_H
#define DB_ENGINE_H

#include "common/macros.h"

#include <memory>
#include <string>
#include <vector>

namespace kvs {

class LSM;
class TransactionManager;

// TODO(namnh) : Do we actually need this class?
// Currently, it is used with a view to support
// multiple storage engine in the future.
class DB {
  public:
    DB();

    ~DB();

    // Copy constructor and assignment
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    // Move constructor and assigment
    // TODO(namnh) : Recheck this one.
    DB(DB&&) = default;
    LSM& operator=(DB&&) = default;

    Transaction* Begin();

    void BatchGet();

    void Get(std::string_view key, TxnId txn_id);

    void BatchPut();

    void Put();

  private:
    std::unique_ptr<TransactionManager> txn_manager_;

    std::unique_ptr<LSM> lsm_;
};

} // namespace kvs

#endif // DB_LSM_H