#ifndef DB_MEMTABLE_H
#define DB_MEMTABLE_H

#include "db/read_only_memtable.h"
#include "common/macros.h"

namespace kvs {

class ReadOnlyMemtable : public Memtable {
  public:
    ReadOnlyMemtable();

    ~ReadOnlyMemtable() override;

    void BatchGet() override;

    void Get() override;

    void BatchPut() override;

    void Put() = override;


  private:
};

} // namespace kvs

#endif // DB_MEMTABLE_H