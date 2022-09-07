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

## \author Barthelemy von Haller
## \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt
## \brief  Try to find the O2 framework package include dirs and libraries

#
# This script will set the following variables:
#  AliceO2_FOUND - System has AliceO2
#  AliceO2_INCLUDE_DIRS - The AliceO2 include directories
#  AliceO2_LIBRARIES - The libraries needed to use AliceO2
#  AliceO2_DEFINITIONS - Compiler switches required for using AliceO2

# Init
include(FindPackageHandleStandardArgs)

# find includes
find_path(AliceO2_INCLUDE_DIR
  NAMES Headers/DataHeader.h
  HINTS ENV O2_ROOT
  PATH_SUFFIXES "include"
)

set(AliceO2_INCLUDE_DIRS ${AliceO2_INCLUDE_DIR})

# find libraries
find_library(AliceO2_LIBRARY_HEADERS NAMES O2Headers HINTS ${O2_ROOT}/lib ENV LD_LIBRARY_PATH)
find_library(AliceO2_LIBRARY_FRAMEWORK NAMES O2Framework HINTS ${O2_ROOT}/lib ENV LD_LIBRARY_PATH)
find_library(AliceO2_LIBRARY_DATAFORMAT NAMES O2DataFormatsParameters HINTS ${O2_ROOT}/lib ENV LD_LIBRARY_PATH)

set(AliceO2_LIBRARIES
  ${AliceO2_LIBRARY_HEADERS}
  ${AliceO2_LIBRARY_FRAMEWORK}
  ${AliceO2_LIBRARY_DATAFORMAT}
)

# handle the QUIETLY and REQUIRED arguments and set AliceO2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(AliceO2
  REQUIRED_VARS AliceO2_LIBRARY_FRAMEWORK AliceO2_LIBRARY_HEADERS AliceO2_LIBRARY_DATAFORMAT AliceO2_INCLUDE_DIR
  FAIL_MESSAGE "AliceO2 could not be found. Install package AliceO2."
)

if(${ALICEO2_FOUND})
    message(STATUS "AliceO2 found, libraries: ${AliceO2_LIBRARIES}")

    mark_as_advanced(AliceO2_INCLUDE_DIRS AliceO2_LIBRARIES)

    # add targets
    if(NOT TARGET AliceO2::Headers)
        add_library(AliceO2::Headers INTERFACE IMPORTED)
        set_target_properties(AliceO2::Headers PROPERTIES
          INTERFACE_INCLUDE_DIRECTORIES "${AliceO2_INCLUDE_DIRS}"
          INTERFACE_LINK_LIBRARIES "${AliceO2_LIBRARY_HEADERS}"
        )
    endif()

    # Following targets we treat as include only libraries to avoid inking against ROOT, GPU libs, etc...

    if(NOT TARGET AliceO2::Framework)
        add_library(AliceO2::Framework INTERFACE IMPORTED)
        set_target_properties(AliceO2::Framework PROPERTIES
          INTERFACE_INCLUDE_DIRECTORIES "${AliceO2_INCLUDE_DIRS}"
          # INTERFACE_LINK_LIBRARIES "${AliceO2_LIBRARY_FRAMEWORK}"
        )
    endif()

    if(NOT TARGET AliceO2::DataFormatsParameters)
        add_library(AliceO2::DataFormatsParameters INTERFACE IMPORTED)
        set_target_properties(AliceO2::DataFormatsParameters PROPERTIES
          INTERFACE_INCLUDE_DIRECTORIES "${AliceO2_INCLUDE_DIRS}"
          # INTERFACE_LINK_LIBRARIES "${AliceO2_LIBRARY_DATAFORMAT}"
        )
    endif()

endif()
