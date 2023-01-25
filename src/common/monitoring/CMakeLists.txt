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
## \brief  DataDistribution monitoring lib

#-------------------------------------------------------------------------------
set (LIB_MON_SOURCES
  DataDistMonitoring.cxx
)

add_library(monitoring OBJECT ${LIB_MON_SOURCES})

target_include_directories(monitoring
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(monitoring
  PRIVATE
    base
    discovery
  PUBLIC
    AliceO2::Monitoring
)