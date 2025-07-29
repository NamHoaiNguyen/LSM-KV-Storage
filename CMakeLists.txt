cmake_minimum_required(VERSION 3.22)
project(LsmKvProject)

set(CMAKE_CXX_STANDARD 20)

# add_compile_options(-Wall -Wextra -Werror)

# Add subdirectories
add_subdirectory(common)
add_subdirectory(concurrency)
add_subdirectory(db)
add_subdirectory(io)
add_subdirectory(sstable)
add_subdirectory(storage)

# add_executable(lsm_kv_storage main.cc)
# target_link_libraries(lsm_kv_storage common concurrency db io sstable storage)
