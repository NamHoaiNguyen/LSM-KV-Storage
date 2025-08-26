#ifndef DB_VALUE_TYPE_H
#define DB_VALUE_TYPE_H

#include <optional>
#include <string>

enum class ValueType : uint8_t {
  PUT = 0,

  DELETED = 1,

  NOT_FOUND = 2,
};

struct GetStatus {
  GetStatus() = default;

  ~GetStatus() = default;

  GetStatus(GetStatus &&) = default;
  GetStatus &operator=(GetStatus &&) = default;

  bool operator==(const GetStatus &&other) const {
    return type == other.type && value == other.value;
  }

  ValueType type;
  std::optional<std::string> value;
};

#endif // DB_VALUE_TYPE_H