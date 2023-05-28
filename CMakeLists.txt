cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(rocksdb-profiler)
include(GNUInstallDirs)
include_directories(${PROJECT_SOURCE_DIR}/include/rocksdbprofiler)
# set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -w")

# find_package(prometheus-cpp CONFIG REQUIRED)
# find_package(Threads REQUIRED)

# include(CMakePrintHelpers)

# file(GLOB SRC_FILES "*/io_monitor.cc")
# file(GLOB HEADERS "include/*")
# add_library(rocksprofiler ${SRC_FILES})
# add_executable(main main.cc)
# target_link_libraries(rocksprofiler ${CMAKE_THREAD_LIBS_INIT})
# target_link_libraries(main rocksprofiler prometheus-cpp::pull)
# cmake_print_variables(CMAKE_INSTALL_INCLUDEDIR)
install(
  DIRECTORY "${PROJECT_SOURCE_DIR}/include/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
# install(
#   TARGETS rocksprofiler
#   EXPORT rocksprofiler-targets
#   LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
# )