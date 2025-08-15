add_library(io
  base_file.h
  linux_file.cc
  linux_file.h
)

target_include_directories(io PUBLIC ${CMAKE_SOURCE_DIR})
# target_include_directories(db PUBLIC ${PROJECT_SOURCE_DIR})
# target_link_libraries(db PUBLIC concurrency)