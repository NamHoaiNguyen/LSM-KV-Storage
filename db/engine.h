#ifndef DB_ENGINE_H
#define DB_ENGINE_H

#include "common/macros.h"

#include <memory>
#include <string>
#include <vector>

namespace kvs {

class LSM;

class Engine {
  public:
    Engine();

    ~Engine();

    // Copy constructor and assignment
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // Move constructor and assigment
    // TODO(namnh) : Recheck this one.
    Engine(Engine&&) = default;
    LSM& operator=(Engine&&) = default;

    void Get();

  private:
    std::unique_ptr<LSM> lsm_;
};

} // namespace kvs

#endif // DB_LSM_H