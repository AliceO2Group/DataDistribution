# - Try to find the UCX libraries, include dirs and libraries
# Author: Gvozden Neskovic
#
# This script will set the following variables:
#  UCX_FOUND - System has ucx
#  UCX_INCLUDE_DIR - The ucx include directories
#  UCX_LIBRARIES - The libraries needed to use ucx


include(FindPackageHandleStandardArgs)

# find includes
find_path(UCX_INCLUDE_DIR
  NAMES ucp/api/ucp.h
  HINTS ${UCX_ROOT}
  PATH_SUFFIXES "include"
)

find_library(UCX_UCP_LIBRARY
    NAMES ucp libucp ucs libucs ucm libucm uct libuct
    HINTS ${UCX_ROOT}/lib ${UCX_ROOT}/lib64
    ENV LD_LIBRARY_PATH
)

find_library(UCX_UCT_LIBRARY
    NAMES uct libuct
    HINTS ${UCX_ROOT}/lib ${UCX_ROOT}/lib64
    ENV LD_LIBRARY_PATH
)

find_library(UCX_UCS_LIBRARY
    NAMES ucs libucs
    HINTS ${UCX_ROOT}/lib ${UCX_ROOT}/lib64
    ENV LD_LIBRARY_PATH
)

find_library(UCX_UCM_LIBRARY
    NAMES ucm libucm
    HINTS ${UCX_ROOT}/lib ${UCX_ROOT}/lib64
    ENV LD_LIBRARY_PATH
)


find_package_handle_standard_args(UCX
    REQUIRED_VARS
      UCX_INCLUDE_DIR
      UCX_UCP_LIBRARY
      UCX_UCT_LIBRARY
      UCX_UCS_LIBRARY
      UCX_UCM_LIBRARY
    HANDLE_COMPONENTS
)

if (UCX_FOUND)
  message(STATUS "Found UCX  (include: ${UCX_INCLUDE_DIR}, library: ${UCX_LIBRARY_DIR})")
  mark_as_advanced(UCX_INCLUDE_DIR UCX_UCP_LIBRARY UCX_UCT_LIBRARY UCX_UCS_LIBRARY UCX_UCM_LIBRARY)
endif ()


if (UCX_FOUND AND NOT TARGET UCX::ucp)
  add_library(UCX::ucp SHARED IMPORTED)
  set_property(TARGET UCX::ucp PROPERTY IMPORTED_LOCATION ${UCX_UCP_LIBRARY})
  target_include_directories(UCX::ucp INTERFACE ${UCX_INCLUDE_DIR})
endif()

if (UCX_FOUND AND NOT TARGET UCX::uct)
  add_library(UCX::uct SHARED IMPORTED)
  set_property(TARGET UCX::uct PROPERTY IMPORTED_LOCATION ${UCX_UCT_LIBRARY})
  target_include_directories(UCX::uct INTERFACE ${UCX_INCLUDE_DIR})
endif()

if (UCX_FOUND AND NOT TARGET UCX::ucm)
  add_library(UCX::ucm SHARED IMPORTED)
  set_property(TARGET UCX::ucm PROPERTY IMPORTED_LOCATION ${UCX_UCM_LIBRARY})
  target_include_directories(UCX::ucm INTERFACE ${UCX_INCLUDE_DIR})
endif()

if (UCX_FOUND AND NOT TARGET UCX::ucs)
  add_library(UCX::ucs SHARED IMPORTED)
  set_property(TARGET UCX::ucs PROPERTY IMPORTED_LOCATION ${UCX_UCS_LIBRARY})
  target_include_directories(UCX::ucs INTERFACE ${UCX_INCLUDE_DIR})
endif()