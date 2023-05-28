cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(rocksdb-profiler)
include(GNUInstallDirs)
include_directories(${PROJECT_SOURCE_DIR}/include/rocksdbprofiler)
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -w")
set(CMAKE_CXX_STANDARD 17)

find_package(prometheus-cpp CONFIG REQUIRED)
find_package(Threads REQUIRED)

include(CMakePrintHelpers)

file(GLOB SRC_FILES "src/*.cc")
add_library(rocksprofiler ${SRC_FILES})
add_executable(main main.cc)
target_link_libraries(rocksprofiler PRIVATE ${CMAKE_THREAD_LIBS_INIT} prometheus-cpp::pull)
target_link_libraries(main rocksprofiler)
install(
  DIRECTORY "${PROJECT_SOURCE_DIR}/include/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(
  TARGETS rocksprofiler
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)