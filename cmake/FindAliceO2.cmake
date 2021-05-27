# - Try to find the O2 framework package include dirs and libraries
# Author: Barthelemy von Haller
# Author: Gvozden Neskovic
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

set(AliceO2_LIBRARIES
  ${AliceO2_LIBRARY_HEADERS}
  ${AliceO2_LIBRARY_FRAMEWORK}
)

# handle the QUIETLY and REQUIRED arguments and set AliceO2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(AliceO2
  REQUIRED_VARS AliceO2_LIBRARY_FRAMEWORK AliceO2_LIBRARY_HEADERS AliceO2_INCLUDE_DIR
  FAIL_MESSAGE "AliceO2 could not be found. Install package AliceO2."
)

if(${ALICEO2_FOUND})
    message(STATUS "AliceO2 found, libraries: ${AliceO2_LIBRARIES}")

    mark_as_advanced(AliceO2_INCLUDE_DIRS AliceO2_LIBRARIES)

    # add targets
    if(NOT TARGET AliceO2::Framework)
        add_library(AliceO2::Framework INTERFACE IMPORTED)
        set_target_properties(AliceO2::Framework PROPERTIES
          INTERFACE_INCLUDE_DIRECTORIES "${AliceO2_INCLUDE_DIRS}"
          INTERFACE_LINK_LIBRARIES "${AliceO2_LIBRARY_FRAMEWORK}"
        )
    endif()

    if(NOT TARGET AliceO2::Headers)
        add_library(AliceO2::Headers INTERFACE IMPORTED)
        set_target_properties(AliceO2::Headers PROPERTIES
          INTERFACE_INCLUDE_DIRECTORIES "${AliceO2_INCLUDE_DIRS}"
          INTERFACE_LINK_LIBRARIES "${AliceO2_LIBRARY_HEADERS}"
        )
    endif()

endif()
