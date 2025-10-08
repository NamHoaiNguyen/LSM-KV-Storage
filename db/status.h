#ifndef DB_VALUE_TYPE_H
#define DB_VALUE_TYPE_H

#include <optional>
#include <string>

namespace kvs {

namespace db {

enum class ValueType : uint8_t {
  PUT = 0,

  DELETED = 1,

  NOT_FOUND = 2,

  INVALID = 3,

  kTooManyOpenFiles = 4,
};

struct GetStatus {
  GetStatus() = default;

  ~GetStatus() = default;

  GetStatus(const GetStatus &) = default;
  GetStatus &operator=(GetStatus &) = default;

  GetStatus(GetStatus &&) = default;
  GetStatus &operator=(GetStatus &&) = default;

  bool operator==(const GetStatus &&other) const {
    return type == other.type && value == other.value;
  }

  ValueType type{ValueType::NOT_FOUND};

  std::optional<std::string> value{std::nullopt};
};

} // namespace db

} // namespace kvs

#endif // DB_VALUE_TYPE_H