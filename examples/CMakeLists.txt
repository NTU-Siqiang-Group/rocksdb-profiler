find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h PATHS)
include_directories(${ROCKSDB_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/include)

find_library(ROCKSDB_LIB rocksdb)
find_package(gflags REQUIRED)

file(GLOB SRC *.cc)
foreach (file ${SRC})
  get_filename_component(exename ${file} NAME_WE)
  add_executable(${exename} ${file})
  target_link_libraries(${exename} ${ROCKSDB_LIB} rocksprofiler prometheus-cpp::pull gflags)
endforeach ()