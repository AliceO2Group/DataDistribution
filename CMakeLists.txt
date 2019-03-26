cmake_minimum_required(VERSION 3.12.2 FATAL_ERROR)

project(DataDistribution
  VERSION 0.5.1
  DESCRIPTION "O2 data distribution components"
  LANGUAGES CXX
)

enable_testing()

cmake_policy(SET CMP0074 NEW)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

message(STATUS "Install directory = ${CMAKE_INSTALL_PREFIX}")

#
#--- FLAGS ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "-Wall -Wextra -pedantic")
set(CMAKE_CXX_FLAGS_DEBUG "-g3")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g3 -O2 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

set(CMAKE_EXE_LINKER_FLAGS "-Wl,--as-needed")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/bin")

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-Wlogical-op -Wnull-dereference -Wuseless-cast" CXX_GOOD_FLAGS)
if(${CXX_GOOD_FLAGS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wlogical-op -Wnull-dereference -Wuseless-cast")
endif()

check_cxx_compiler_flag("-Wduplicated-cond -Wdouble-promotion -Wshadow -Wformat=2" CXX_GOOD_FLAGS)
if(${CXX_GOOD_FLAGS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wduplicated-cond -Wdouble-promotion -Wshadow -Wformat=2")
endif()

check_cxx_compiler_flag("-frecord-gcc-switches" CXX_GOOD_FLAGS)
if(${CXX_GOOD_FLAGS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -frecord-gcc-switches")
endif()

#
#--- DEFAULT BLD TYPE  ---------------------------------------------------------------
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo"
    CACHE
    STRING "Choose the type of build, options are: Debug Release RelWithDebInfo."
    FORCE
  )
endif()

#
#--- DEPENDENCIES --------------------------------------------------------------------

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

find_package(FairMQ 1.4.2 REQUIRED)
find_package(FairLogger ${FairMQ_FairLogger_VERSION} REQUIRED)

# Boost
# Inherit FairMQ Boost deps and add ours
set(Boost_COMPONENTS ${FairMQ_Boost_COMPONENTS})
list(INSERT Boost_COMPONENTS
  range
  filesystem
  unit_test_framework
)
list(REMOVE_DUPLICATES Boost_COMPONENTS)

find_package(Boost ${FairMQ_Boost_VERSION} REQUIRED COMPONENTS ${Boost_COMPONENTS})

# find GSL (https://github.com/Microsoft/GSL)
find_path(MS_GSL_INCLUDE_DIR
  NAMES gsl/gsl
  HINTS ${MS_GSL_ROOT} ENV MS_GSL_ROOT
  PATH_SUFFIXES "include"
)

find_package(AliceO2 REQUIRED)
find_package(Monitoring REQUIRED)
find_package(ROOT REQUIRED)


message(STATUS "Boost version : ${Boost_VERSION}")
message(STATUS "Boost include path : ${Boost_INCLUDE_DIRS}")
message(STATUS "FairMQ version : ${FairMQ_VERSION}")
message(STATUS "AliceO2 include path : ${AliceO2_INCLUDE_DIR}")

add_subdirectory(src)
add_subdirectory(script)
add_subdirectory(doc)
