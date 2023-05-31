cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(rocksdb-profiler)
include(GNUInstallDirs)
include_directories(${PROJECT_SOURCE_DIR}/include/rocksdbprofiler)

option(BUILD_EXAMPLES "Build examples" OFF)
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -w")
set(CMAKE_CXX_STANDARD 17)

find_package(prometheus-cpp CONFIG REQUIRED)
find_package(Threads REQUIRED)

include(CMakePrintHelpers)

file(GLOB SRC_FILES "src/*.cc")
add_library(rocksprofiler ${SRC_FILES})
target_link_libraries(rocksprofiler PRIVATE ${CMAKE_THREAD_LIBS_INIT} prometheus-cpp::pull)
install(
  DIRECTORY "${PROJECT_SOURCE_DIR}/include/"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(
  TARGETS rocksprofiler
  EXPORT rocksprofiler
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
)

install(
  EXPORT rocksprofiler
  FILE rocksprofiler.cmake
  NAMESPACE rocksprofiler::
  DESTINATION lib/cmake/rocksprofiler)

include(CMakePackageConfigHelpers)

configure_package_config_file(
  cmake/rocksprofilerConfig.cmake.in ${PROJECT_BINARY_DIR}/cmake/rocksprofilerConfig.cmake
  INSTALL_DESTINATION lib/cmake/rocksprofiler
)

install(
  FILES
  "${PROJECT_BINARY_DIR}/cmake/rocksprofilerConfig.cmake"
  DESTINATION lib/cmake/rocksprofiler
)

if (BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()