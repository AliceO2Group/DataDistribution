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

add_subdirectory(common)
add_subdirectory(StfBuilder)

if (UCX_FOUND)
    add_subdirectory(ReadoutEmulator)
    add_subdirectory(StfSender)
    add_subdirectory(TfBuilder)
    add_subdirectory(TfScheduler)
    add_subdirectory(DataDistControl)
endif()

add_subdirectory(tests)
