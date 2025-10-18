#include "db/db_impl.h"

#include <iostream>

using namespace kvs;

int main() {
  kvs::db::DBImpl db;
  bool success = db.LoadDB("test");
  if (!success) {
    return 0;
  }

  // Put data
  db.Put("key1", "value1");
  db.Put("thunder", "colossus");
  db.Put("A random key!!!", "random value");

  // Query data;
  auto status = db.Get("key1");
  if (status.value.has_value()) {
    std::cout << "key1: " << status.value.value() << std::endl;
  } else {
    std::cout << "key1 not found" << std::endl;
  }
  status = db.Get("A random key!!!");
  if (status.value.has_value()) {
    std::cout << "A random key!!!: " << status.value.value() << std::endl;
  } else {
    std::cout << "key not found" << std::endl;
  }
  status = db.Get("thunder");
  if (status.value.has_value()) {
    std::cout << "thunder: " << status.value.value() << std::endl;
  } else {
    std::cout << "key not found" << std::endl;
  }

  // Delete key
  db.Delete("thunder");
  status = db.Get("thunder");
  if (status.value.has_value()) {
    std::cout << "thunder: " << status.value.value() << std::endl;
  } else {
    std::cout << "key not found" << std::endl;
  }

  // Update a key
  db.Put("key1", "new value updated by key1");
  status = db.Get("key1");
  if (status.value.has_value()) {
    std::cout << "key1: " << status.value.value() << std::endl;
  } else {
    std::cout << "key not found" << std::endl;
  }

  return 0;
}