add_library(db
  base_memtable.h
  db.cc
  db.h
  lsm.cc
  lsm.h
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
)

target_include_directories(db PUBLIC ${CMAKE_SOURCE_DIR})
# target_include_directories(db PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(db PUBLIC concurrency)