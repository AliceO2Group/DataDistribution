# Copyright 2019-2022 CERN and copyright holders of ALICE O2.
# See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
# All rights not expressly granted are reserved.
#
# This software is distributed under the terms of the GNU General Public
# License v3 (GPL Version 3), copied verbatim in the file "COPYING".
#
# In applying this license CERN does not waive the privileges and immunities
# granted to it by virtue of its status as an Intergovernmental Organization
# or submit itself to any jurisdiction.

## \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

cmake_minimum_required(VERSION 3.13 FATAL_ERROR)

project(DataDistribution
  DESCRIPTION "O2 data distribution components"
  LANGUAGES CXX
)

enable_testing()

cmake_policy(SET CMP0074 NEW)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

cmake_policy(SET CMP0065 NEW)

message(STATUS "Install directory = ${CMAKE_INSTALL_PREFIX}")

#
#--- FLAGS ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -fno-inline -fno-omit-frame-pointer -fmax-errors=10")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -ffast-math -DNDEBUG -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -ffast-math -DNDEBUG")

if(NOT APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pedantic -Werror")
endif()


set(EXECUTABLE_OUTPUT_PATH "${CMAKE_BINARY_DIR}/bin")

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-Wlogical-op -Wnull-dereference -Wuseless-cast" CXX_GOOD_FLAGS)
if(${CXX_GOOD_FLAGS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wlogical-op -Wnull-dereference")
endif()

check_cxx_compiler_flag("-Wl,--no-as-needed -ldl" CXX_GOOD_FLAGS)
if(${CXX_GOOD_FLAGS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--no-as-needed -ldl")
endif()

check_cxx_compiler_flag("-Wduplicated-cond -Wdouble-promotion -Wshadow -Wformat=2" CXX_GOOD_FLAGS)
if(${CXX_GOOD_FLAGS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wduplicated-cond -Wdouble-promotion -Wshadow -Wformat=2")
endif()

check_cxx_compiler_flag("-frecord-gcc-switches" CXX_GOOD_FLAGS)
if(${CXX_GOOD_FLAGS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -frecord-gcc-switches")
endif()

check_cxx_compiler_flag("-fvar-tracking-assignments" CXX_GOOD_FLAGS)
if(${CXX_GOOD_FLAGS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvar-tracking-assignments -gdwarf-4")
else()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb3")
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
message(STATUS "Looking for dependencies.")
message(STATUS "CMAKE_CXX_FLAGS = ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_STANDARD = ${CMAKE_CXX_STANDARD}")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

find_package(Threads REQUIRED)
find_package(FairMQ 1.4.41 REQUIRED)
find_package(FairLogger ${FairMQ_FairLogger_VERSION} REQUIRED)
message(STATUS "Looking for FairLogger dependencies: ${FairLogger_PACKAGE_DEPENDENCIES}" )
foreach(dep IN LISTS FairLogger_PACKAGE_DEPENDENCIES)
  find_package(${dep} ${FairLogger_${dep}_VERSION})
endforeach()

# Boost
# Inherit FairMQ Boost deps and add ours
set(Boost_COMPONENTS ${FairMQ_Boost_COMPONENTS})
list(APPEND Boost_COMPONENTS
  filesystem
  iostreams
  unit_test_framework
)
list(REMOVE_DUPLICATES Boost_COMPONENTS)
find_package(Boost ${FairMQ_Boost_VERSION} REQUIRED COMPONENTS ${Boost_COMPONENTS})

find_package(ppconsul REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

find_package(AliceO2 REQUIRED)
find_package(Monitoring REQUIRED) # AliceO2::Monitoring

find_package(InfoLogger REQUIRED CONFIG NAMES InfoLogger libInfoLogger)

# see if our 3rd parties are Installed
# note: spdlog builds against external fmt because FairLogger does the same

find_package(spdlog
  HINTS "${CMAKE_BINARY_DIR}/spdlog"
)
if (NOT spdlog_FOUND)
  configure_file(
    ${CMAKE_SOURCE_DIR}/cmake/CMakeLists.txt.in
    3rdparty/CMakeLists.txt
  )

  execute_process(
    COMMAND ${CMAKE_COMMAND} -G "Unix Makefiles" .
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/3rdparty
  )
  execute_process(
    COMMAND ${CMAKE_COMMAND} --build . --parallel 2
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/3rdparty
  )

  # make sure we succeeded
  find_package(spdlog REQUIRED
    HINTS "${CMAKE_BINARY_DIR}/spdlog"
  )
endif()

include(jemalloc)


find_package(UCX)
# NOTE: Determines if we build StfSender, TfBuilder and TfScheduler
#       StfBuilder is always built

message(STATUS "Boost version : ${Boost_VERSION}")
message(STATUS "Boost include path : ${Boost_INCLUDE_DIRS}")
message(STATUS "jemalloc include : ${jemalloc_INCLUDE_DIRS}")
message(STATUS "FairMQ version : ${FairMQ_VERSION}")
message(STATUS "AliceO2 include path : ${AliceO2_INCLUDE_DIR}")
message(STATUS "UCX include path : ${UCX_INCLUDE_DIR}")

add_subdirectory(src)
add_subdirectory(doc)
