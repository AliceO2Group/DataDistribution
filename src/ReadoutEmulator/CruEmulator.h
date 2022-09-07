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

#ifndef ALICEO2_CRU_EMULATOR_H_
#define ALICEO2_CRU_EMULATOR_H_

#include "CruMemoryHandler.h"

#include <ConcurrentQueue.h>
#include <ReadoutDataModel.h>

#include <Headers/DataHeader.h>

#include <stack>
#include <map>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>

namespace o2::DataDistribution
{

struct RawDmaChunkDesc {
  uint64_t mHBFrameID; // unused
  size_t mRawDataSize; // unused
  bool mValidHBF;
};

class CruLinkEmulator
{
 public:
  CruLinkEmulator(std::shared_ptr<CruMemoryHandler> pMemHandler, uint64_t pLinkId, uint64_t pLinkBitsPerS, uint64_t pDmaChunkSize, size_t pOrbitsInTf)
    : mMemHandler{ pMemHandler },
      mLinkID{ pLinkId },
      mLinkBitsPerS{ pLinkBitsPerS },
      mDmaChunkSize{ pDmaChunkSize },
      mOrbitsInTf{ pOrbitsInTf },
      mRunning{ false }
  {
  }

  ~CruLinkEmulator()
  {
    stop();
  }

  void linkReadoutThread();

  /// Start "data taking" thread
  void start();
  /// Stop "data taking" thread
  void stop();

 private:
  std::shared_ptr<CruMemoryHandler> mMemHandler;

  std::uint64_t mLinkID;
  std::uint64_t mLinkBitsPerS;
  std::uint64_t mDmaChunkSize;
  std::uint64_t mOrbitsInTf;

  std::thread mCRULinkThread;
  bool mRunning;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_CRU_EMULATOR_H_ */
