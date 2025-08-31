add_library(mvcc
  transaction_manager.cc
  transaction_manager.h
  transaction.cc
  transaction.h
)

target_include_directories(mvcc PUBLIC ${CMAKE_SOURCE_DIR})
