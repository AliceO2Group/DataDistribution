// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "CruMemoryHandler.h"
#include "CruEmulator.h"
#include "DataDistLogger.h"

#include <ConcurrentQueue.h>

#include <fairmq/FairMQUnmanagedRegion.h>
#include <fairmq/FairMQDevice.h> /* NewUnmanagedRegionFor */
#include <fairmq/ProgOptions.h>

#include <chrono>
#include <thread>

namespace o2::DataDistribution
{

void CruMemoryHandler::teardown()
{
  mO2LinkDataQueue.stop(); // get will not block, return false
}

void CruMemoryHandler::init(DataRegionAllocatorResource *pDataRegion, std::size_t pSuperPageSize)
{
  mSuperpageSize = pSuperPageSize;
  mDataRegion = pDataRegion;

  const auto lCntSuperpages = getDataRegionSize() / mSuperpageSize;

  IDDLOG("CRU Memory Handler initialization finished. Using {} superpages.", lCntSuperpages);
}

bool CruMemoryHandler::getSuperpage(CRUSuperpage& sp)
{
  std::scoped_lock lLock(mDataRegionLock);
  char *lPtr = reinterpret_cast<char*>(mDataRegion->do_allocate(mSuperpageSize));
  if (lPtr) {
    sp.mDataVirtualAddress = lPtr;
    return true;
  }

  return false;
}

size_t CruMemoryHandler::free_superpages()
{
  return mDataRegion->free() / mSuperpageSize;
}

} /* namespace o2::DataDistribution */
