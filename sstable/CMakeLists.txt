add_library(sstable
  block_builder.cc
  block_builder.h
  block.cc
  block.h
  table_builder.cc
  table_builder.h
  table.cc
  table.h
)

target_include_directories(sstable PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(sstable PUBLIC concurrency io)