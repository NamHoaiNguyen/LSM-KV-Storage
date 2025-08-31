add_library(db
  base_memtable.h
  compact.cc
  compact.h
  db_impl.cc
  db_impl.h
  memtable_iterator.cc
  memtable_iterator.h
  memtable.cc
  memtable.h
  skiplist_iterator.cc
  skiplist_iterator.h
  skiplist_node.cc
  skiplist_node.h
  skiplist.cc
  skiplist.h
  status.h
)

target_include_directories(db PUBLIC ${CMAKE_SOURCE_DIR})
# target_include_directories(db PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(db PUBLIC io mvcc sstable)