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

#include "CruEmulator.h"
#include "DataDistLogger.h"

#include <ConcurrentQueue.h>

#include <fairmq/ProgOptions.h>

#include <chrono>
#include <thread>

#include <unistd.h>

#include <Headers/DAQID.h>

namespace o2::DataDistribution
{

void CruLinkEmulator::linkReadoutThread()
{
  static const std::uint64_t cHBFrameFreq = 11223;

  const auto cSuperpageSize = mMemHandler->getSuperpageSize();
  const auto cHBFrameSize = (mLinkBitsPerS / cHBFrameFreq) >> 3;
  const auto cStfLinkSize = cHBFrameSize * cHBFrameFreq * mOrbitsInTf / cHBFrameFreq;
  const auto cNumDmaChunkPerSuperpage = std::min(size_t(mOrbitsInTf), size_t(cSuperpageSize / mDmaChunkSize));
  const int64_t cStfTimeUs = std::chrono::microseconds(std::uint64_t(1000000) * mOrbitsInTf / cHBFrameFreq).count();

  DDDLOG("Superpage size: {}", cSuperpageSize);
  DDDLOG("mDmaChunkSize size: {}", mDmaChunkSize);
  DDDLOG("HBFrameSize size: {}", cHBFrameSize);
  DDDLOG("StfLinkSize size: {}", cStfLinkSize);
  DDDLOG("cNumDmaChunkPerSuperpage: {}", cNumDmaChunkPerSuperpage);
  DDDLOG("Sleep time us: {}", cStfTimeUs);

  // os might sleep much longer than requested
  // keep count of transmitted pages and adjust when needed
  int64_t lSentStf = 0;
  using hres_clock = std::chrono::steady_clock;
  const auto lOpStart = hres_clock::now();

  std::vector<CRUSuperpage> lSuperpages;

  while (mRunning) {

    const int64_t lUsSinceStart = since<std::chrono::microseconds>(lOpStart);
    const int64_t lStfToSend = lUsSinceStart / cStfTimeUs - lSentStf;

    if (lStfToSend <= 0) {
      usleep(500);
      continue;
    }

    if (lStfToSend > 1 || lStfToSend < 0) {
      WDDLOG_GRL(10000, "Data producer is running slow. StfBacklog: {}", lStfToSend);
    }

    const std::int64_t lPagesToSend = std::max(lStfToSend, int64_t(lStfToSend * (cStfLinkSize + cSuperpageSize - 1) / cSuperpageSize));

    // request enough superpages (can be less!)
    std::int64_t lPagesAvail = lSuperpages.size();
    if (lPagesAvail < lPagesToSend) {
      lPagesAvail += mMemHandler->getSuperpages(std::max(lPagesToSend, std::int64_t(32)), std::back_inserter(lSuperpages));
      assert(std::size_t(lPagesAvail) == lSuperpages.size());
    }
    lPagesAvail = lSuperpages.size();

    for (int64_t stf = 0; stf < lStfToSend; stf++, lSentStf++) {
      auto lHbfToSend = mOrbitsInTf;

      while (lHbfToSend > 0) {
        if (!lSuperpages.empty()) {
          CRUSuperpage sp = lSuperpages.back();
          lSuperpages.pop_back();

          // Enumerate valid data and create work-item for STFBuilder
          // Each channel is reported separately to the O2
          ReadoutLinkO2Data linkO2Data;

          linkO2Data.mLinkHeader.mSystemId = (rand() % 100 < 70) ? o2::header::DAQID::TPC : o2::header::DAQID::ITS;
          linkO2Data.mLinkHeader.mFeeId = 0xFEE0;
          linkO2Data.mLinkHeader.mEquipmentId = 0xE1D0; // ?
          linkO2Data.mLinkHeader.mLinkId = mLinkID;
          linkO2Data.mLinkHeader.mFlags.mIsRdhFormat = 1;

          for (unsigned d = 0; d < cNumDmaChunkPerSuperpage; d++, lHbfToSend--) {

            if (lHbfToSend == 0)
              break; // start a new superpage

            // make set few bits for the RDHv4 packet to keep SubTimeFrame builder happy
            {
              char *lRdh = reinterpret_cast<char*>(sp.mDataVirtualAddress + (d * mDmaChunkSize));
              // version
              std::uint32_t lVer = 0x00000004;
              std::memcpy(lRdh, &lVer, sizeof(std::uint32_t));

              // cru, linkid, ep
              std::uint32_t lEquipment = 0;
              lEquipment = (
                (std::uint32_t(0xEEE) << 16) |  /* CRU, 12bit */
                (std::uint32_t(0x0) << 28) |    /* endpoint id */
                (std::uint32_t(mLinkID) & 0xFF) /* linkID, 8 bit */
              );

              std::memcpy(lRdh+12, &lEquipment, sizeof(std::uint32_t));
            }

            assert (d * mDmaChunkSize < cSuperpageSize);
            linkO2Data.mLinkRawData.push_back(CruDmaPacket{
              mMemHandler->getDataRegion(),
              sp.mDataVirtualAddress + (d * mDmaChunkSize),        // Valid data DMA Chunk <superpage offset + length>
              mDmaChunkSize /* - (rand() % (mDmaChunkSize / 10))*/ // This should be taken from desc->mRawDataSize (filled by the CRU)
            });
          }

          // Put the link info data into the send queue
          mMemHandler->putLinkData(std::move(linkO2Data));

        } else {
          // signal lost data (no free superpages)
          ReadoutLinkO2Data linkO2Data;
          linkO2Data.mLinkHeader.mFlags.mIsRdhFormat = 0;

          mMemHandler->putLinkData(std::move(linkO2Data));
          break;
        }
      }
    }
  }

  DDDLOG("Exiting ReadoutEmulator thread.");
}

/// Start "data taking" thread
void CruLinkEmulator::start()
{
  mRunning = true;
  mCRULinkThread = create_thread_member("cru_link", &CruLinkEmulator::linkReadoutThread, this);
}

/// Stop "data taking" thread
void CruLinkEmulator::stop()
{
  mRunning = false;
  if (mCRULinkThread.joinable()) {
    mCRULinkThread.join();
  }
}

} /* namespace o2::DataDistribution */
