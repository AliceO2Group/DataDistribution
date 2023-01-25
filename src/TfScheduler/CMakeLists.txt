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
## \brief  cmake for TfScheduler

set(EXE_TFS_SOURCES
  TfSchedulerDevice.cxx
  TfSchedulerInstance.cxx
  TfSchedulerInstanceRpc.cxx
  TfSchedulerConnManager.cxx
  TfSchedulerTfBuilderInfo.cxx
  TfSchedulerStfInfo.cxx
  TfSchedulerTokenManager.cxx
  runTfScheduler.cxx
)

add_library(TfScheduler_lib OBJECT ${EXE_TFS_SOURCES})
target_link_libraries(TfScheduler_lib  base fmqtools ucxtools discovery rpc monitoring)

add_executable(TfScheduler)

include(CheckIPOSupported)
check_ipo_supported(RESULT result)
if(result)
  set_target_properties(TfScheduler PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

target_link_libraries(TfScheduler
  PRIVATE
    TfScheduler_lib
    base fmqtools ucxtools discovery rpc monitoring
)

install(TARGETS TfScheduler RUNTIME DESTINATION bin)
