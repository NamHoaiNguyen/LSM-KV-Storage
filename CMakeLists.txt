cmake_minimum_required(VERSION 3.22)
project(LsmKvProject)

set(CMAKE_CXX_STANDARD 20)

# add_compile_options(-Wall -Wextra -Werror)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_C_FLAGS_DEBUG "-g -O0")

# Add subdirectories
# add_subdirectory(common)
add_subdirectory(db)
add_subdirectory(io)
add_subdirectory(mvcc)
add_subdirectory(sstable)
add_subdirectory(third_party)
# add_subdirectory(storage)

# Add example
add_subdirectory(example)

# add_executable(lsm_kv_storage main.cc)
# target_link_libraries(lsm_kv_storage common mvcc db io sstable storage)

# Add GoogleTest
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_MakeAvailable(googletest)

enable_testing()
add_subdirectory(tests)