// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

#ifndef ALICEO2_CRU_MEMORY_HANDLER_H_
#define ALICEO2_CRU_MEMORY_HANDLER_H_

#include <ConcurrentQueue.h>
#include <ReadoutDataModel.h>

#include "Headers/DataHeader.h"

#include <MemoryUtils.h>

#include <stack>
#include <map>
#include <vector>
#include <queue>
#include <iterator>

#include <mutex>
#include <condition_variable>
#include <thread>

#include <functional>

#include <fairmq/FwdDecls.h>

namespace o2::DataDistribution
{

using namespace o2::header;

struct CRUSuperpage {
  char* mDataVirtualAddress;
};

struct CruDmaPacket {
  DataRegionAllocatorResource *mDataSHMRegion = nullptr;
  char *mDataPtr = nullptr;
  size_t mDataSize = size_t(0);
};

struct ReadoutLinkO2Data {
  ReadoutSubTimeframeHeader mLinkHeader;
  std::vector<CruDmaPacket> mLinkRawData;
};

class CruMemoryHandler
{
 public:
  CruMemoryHandler() = default;
  ~CruMemoryHandler()
  {
    teardown();
  }

  void init(DataRegionAllocatorResource *pDataRegion, std::size_t pSuperPageSize);
  void teardown();

  std::size_t getSuperpageSize() const
  {
    return mSuperpageSize;
  }

  // get a superpage from the free list
  bool getSuperpage(CRUSuperpage& sp);

  // get number of superpages from the free list (perf)
  template <class OutputIt>
  std::size_t getSuperpages(unsigned long n, OutputIt spDst)
  {
    std::scoped_lock lLock(mDataRegionLock);
    for (unsigned long i = 0; i < n; i++) {
      void *ptr = mDataRegion->do_allocate(mSuperpageSize);
      if (ptr) {
        spDst = CRUSuperpage{reinterpret_cast<char*>(ptr)};
        spDst++;
      } else {
        return i;
      }
    }

    return n;
  }

  // not useful
  void put_superpage(const char* spVirtAddr);

  size_t free_superpages();

  auto getDataRegion() const
  {
    return mDataRegion;
  }

  char* getDataRegionPtr() const
  {
    std::scoped_lock lLock(mDataRegionLock);
    return reinterpret_cast<char*>(
      (reinterpret_cast<uintptr_t>(mDataRegion->address()) + mSuperpageSize - 1)  & ~(mSuperpageSize - 1)
    );
  }

  auto getDataRegionSize() const
  {
    std::scoped_lock lLock(mDataRegionLock);
    return mDataRegion->size();
  }

  // FIFO of filled ReadoutLinkO2Data updates to be sent to STFBuilder (thread safe)
  // linkThread<1..N> -> queue -> mCruO2InterfaceThread
  void putLinkData(ReadoutLinkO2Data&& pLinkData)
  {
    mO2LinkDataQueue.push(std::move(pLinkData));
  }
  bool getLinkData(ReadoutLinkO2Data& pLinkData)
  {
    return mO2LinkDataQueue.pop(pLinkData);
  }

 private:
  mutable std::mutex mDataRegionLock;
  DataRegionAllocatorResource *mDataRegion;
  std::size_t mSuperpageSize;

  /// output data queue
  ConcurrentFifo<ReadoutLinkO2Data> mO2LinkDataQueue;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_CRU_MEMORY_HANDLER_H_ */
