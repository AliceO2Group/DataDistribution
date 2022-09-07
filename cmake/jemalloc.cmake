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
## \brief  build jemalloc dependency

#--- EXTERNAL PROJECTS  --------------------------------------------------------------
include(ExternalProject)

ExternalProject_Add(jemalloc
  GIT_REPOSITORY "https://github.com/jemalloc/jemalloc.git"
  GIT_TAG "5.3.0"

  GIT_SHALLOW TRUE
  GIT_PROGRESS TRUE

  UPDATE_COMMAND ""
  PATCH_COMMAND ""

  SOURCE_DIR "${CMAKE_BINARY_DIR}/3rdparty/jemalloc"

  BUILD_IN_SOURCE 1
  BUILD_BYPRODUCTS ${jemalloc_STATIC} ${jemalloc_STATIC_PIC}

  CONFIGURE_COMMAND ./autogen.sh
    COMMAND ./configure --disable-shared
                        --disable-doc
                        --prefix=${CMAKE_BINARY_DIR}/jemalloc
                        --with-malloc-conf=abort_conf:true,background_thread:true,metadata_thp:auto,dirty_decay_ms:300000,muzzy_decay_ms:300000
                        "CFLAGS=${CMAKE_C_FLAGS} -Wno-missing-attributes"

  BUILD_COMMAND ${MAKE}
  INSTALL_COMMAND make install

  LOG_DOWNLOAD True
  LOG_UPDATE True
  LOG_INSTALL True
  LOG_OUTPUT_ON_FAILURE True
)

set(jemalloc_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/jemalloc/include)

add_library(jemalloc_STATIC STATIC IMPORTED)
set_property(TARGET jemalloc_STATIC PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/jemalloc/lib/libjemalloc.a)
add_dependencies(jemalloc_STATIC jemalloc)

add_library(jemalloc_STATIC_PIC STATIC IMPORTED)
set_property(TARGET jemalloc_STATIC_PIC PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/jemalloc/lib/libjemalloc_pic.a)
add_dependencies(jemalloc_STATIC_PIC jemalloc)

if (!APPLE)
  link_libraries(-Wl,--no-as-needed)
endif(!APPLE)

link_libraries(dl ${jemalloc_STATIC_PIC})
