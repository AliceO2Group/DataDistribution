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
#include <options/FairMQProgOptions.h>

#include <chrono>
#include <thread>

namespace o2
{
namespace DataDistribution
{

constexpr unsigned long CruMemoryHandler::cBufferBucketSize;

void CruMemoryHandler::teardown()
{
  mO2LinkDataQueue.stop(); // get will not block, return false
  mSuperpages.stop();

  for (unsigned b = 0; b < cBufferBucketSize; b++) {
    std::lock_guard<std::mutex> lock(mBufferMap[b].mLock);
    mBufferMap[b].mVirtToSuperpage.clear();
    mBufferMap[b].mUsedSuperPages.clear();
  }
}

void CruMemoryHandler::init(FairMQUnmanagedRegion* pDataRegion, std::size_t pSuperPageSize)
{
  mSuperpageSize = pSuperPageSize;
  mDataRegion = pDataRegion;

  const auto lCntSuperpages = getDataRegionSize() / mSuperpageSize;

  // lock and initialize the empty page queue
  mSuperpages.flush();

  for (unsigned b = 0; b < cBufferBucketSize; b++) {
    std::lock_guard<std::mutex> lock(mBufferMap[b].mLock);
    mBufferMap[b].mVirtToSuperpage.clear();
    mBufferMap[b].mUsedSuperPages.clear();
  }

  for (size_t i = 0; i < lCntSuperpages; i++) {
    const CRUSuperpage sp{ getDataRegionPtr() + (i * mSuperpageSize), reinterpret_cast<char*>(~0x0) };
    // stack of free superpages to feed the CRU
    if (!sp.mDataVirtualAddress) {
      EDDLOG("init_superpage: Data region pointer null region_ptr={:p}", getDataRegionPtr());
    }
    mSuperpages.push(sp);

    // Virtual address to superpage mapping to help with returning of the used pages
    if (!sp.mDataVirtualAddress) {
      EDDLOG("init_superpage: Data region pointer null region_ptr={:p}", getDataRegionPtr());
    }

    auto& lBucket = getBufferBucket(sp.mDataVirtualAddress);
    std::lock_guard<std::mutex> lock(lBucket.mLock);

    lBucket.mVirtToSuperpage[sp.mDataVirtualAddress] = sp;
  }

  IDDLOG("CRU Memory Handler initialization finished. Using {} superpages.", lCntSuperpages);
}

bool CruMemoryHandler::getSuperpage(CRUSuperpage& sp)
{
  return mSuperpages.try_pop(sp);
}

void CruMemoryHandler::put_superpage(const char* spVirtAddr)
{
  auto& lBucket = getBufferBucket(spVirtAddr);

  std::lock_guard<std::mutex> lock(lBucket.mLock); // needed for the mVirtToSuperpage[] lookup
  const auto lSp = lBucket.mVirtToSuperpage[spVirtAddr];
  if (!lSp.mDataVirtualAddress) {
    EDDLOG("put_superpage: Data region pointer null region_ptr={:p} num_spaages={}",
      getDataRegionPtr(), lBucket.mVirtToSuperpage.size());
  }
  mSuperpages.push(lSp);
}

size_t CruMemoryHandler::free_superpages()
{
  return mSuperpages.size();
}

void CruMemoryHandler::get_data_buffer(const char* dataBufferAddr, const std::size_t dataBuffSize)
{
  const char* lSpStartAddr = reinterpret_cast<char*>((uintptr_t)dataBufferAddr & ~(mSuperpageSize - 1));

  auto& lBucket = getBufferBucket(lSpStartAddr);
  std::lock_guard<std::mutex> lock(lBucket.mLock);

  // make sure the data buffer is not already in use
  if (lBucket.mUsedSuperPages[lSpStartAddr].count(dataBufferAddr) != 0) {
    EDDLOG("Data buffer is already in the used list! addr={:#010X}", dataBufferAddr);
    return;
  }

  lBucket.mUsedSuperPages[lSpStartAddr][dataBufferAddr] = dataBuffSize;
}

void CruMemoryHandler::put_data_buffer(const char* dataBufferAddr, const std::size_t dataBuffSize)
{
  const char* lSpStartAddr = reinterpret_cast<char*>((uintptr_t)dataBufferAddr & ~(mSuperpageSize - 1));

  if (lSpStartAddr < getDataRegionPtr() || lSpStartAddr > getDataRegionPtr() + getDataRegionSize()) {
    EDDLOG("Returned data buffer outside of the data segment!"
      " sp_start={:#010X} b_addr={:#010X} reg_addr={:#010X} reg_end={:#010X}",
      reinterpret_cast<uintptr_t>(lSpStartAddr),
      reinterpret_cast<uintptr_t>(dataBufferAddr),
      reinterpret_cast<uintptr_t>(getDataRegionPtr()),
      reinterpret_cast<uintptr_t>(getDataRegionPtr() + getDataRegionSize()));
    return;
  }

  const auto lDataBufferAddr = dataBufferAddr;
  const auto lDataBuffSize = dataBuffSize;

  auto& lBucket = getBufferBucket(lSpStartAddr);
  std::lock_guard<std::mutex> lock(lBucket.mLock);

  if (lBucket.mUsedSuperPages.count(lSpStartAddr) == 0) {
    EDDLOG_RL(200, "Returned data buffer is not in the list of used superpages!");
    return;
  }

  auto& lSpBuffMap = lBucket.mUsedSuperPages[lSpStartAddr];

  if (lSpBuffMap.count(lDataBufferAddr) == 0) {
    EDDLOG("Returned data buffer is not marked as used within the superpage!");
    return;
  }

  if (lSpBuffMap[lDataBufferAddr] != lDataBuffSize) {
    EDDLOG("Returned data buffer size does not match the records! recorded={} returned={}",
      lSpBuffMap[lDataBufferAddr], lDataBuffSize);
    return;
  }

  if (lSpBuffMap.size() > 1) {
    lSpBuffMap.erase(lDataBufferAddr);
  } else if (lSpBuffMap.size() == 1) {
    lBucket.mUsedSuperPages.erase(lSpStartAddr);

    const auto lSp = lBucket.mVirtToSuperpage[lSpStartAddr];
    if (!lSp.mDataVirtualAddress) {
      EDDLOG("put_data_buffer: Data region pointer null region_ptr={:p} sp_addr={:p} num_spaages={}",
        getDataRegionPtr(), lSpStartAddr, lBucket.mVirtToSuperpage.size());
    }

    mSuperpages.push(lSp);
  } else {
    EDDLOG("Superpage chunk lost.");
  }
}
}
} /* namespace o2::DataDistribution */
