// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "CruEmulator.h"

#include <ConcurrentQueue.h>

#include <options/FairMQProgOptions.h>
#include <FairMQLogger.h>

#include <chrono>
#include <thread>

#include <unistd.h>

namespace o2
{
namespace DataDistribution
{

void CruLinkEmulator::linkReadoutThread()
{
  static const size_t cHBFrameFreq = 11223;
  static const size_t cStfPerS = 43; /* Parametrize this? */

  const auto cSuperpageSize = mMemHandler->getSuperpageSize();
  const auto cHBFrameSize = (mLinkBitsPerS / cHBFrameFreq) >> 3;
  const auto cStfLinkSize = cHBFrameSize * cHBFrameFreq / cStfPerS;
  const auto cNumDmaChunkPerSuperpage = std::min(size_t(256), cSuperpageSize / mDmaChunkSize);
  constexpr int64_t cStfTimeUs = std::chrono::microseconds(1000000 / cStfPerS).count();

  LOG(DEBUG) << "Superpage size: " << cSuperpageSize;
  LOG(DEBUG) << "mDmaChunkSize size: " << mDmaChunkSize;
  LOG(DEBUG) << "HBFrameSize size: " << cHBFrameSize;
  LOG(DEBUG) << "StfLinkSize size: " << cStfLinkSize;
  LOG(DEBUG) << "cNumDmaChunkPerSuperpage: " << cNumDmaChunkPerSuperpage;
  LOG(DEBUG) << "Sleep time us: " << cStfTimeUs;

  // os might sleep much longer than requested
  // keep count of transmitted pages and adjust when needed
  int64_t lSentStf = 0;
  using hres_clock = std::chrono::high_resolution_clock;
  const auto lOpStart = hres_clock::now();

  std::deque<CRUSuperpage> lSuperpages;

  while (mRunning) {

    const int64_t lUsSinceStart = std::chrono::duration_cast<std::chrono::microseconds>(hres_clock::now() - lOpStart).count();
    const int64_t lStfToSend = lUsSinceStart / cStfTimeUs - lSentStf;

    if (lStfToSend <= 0) {
      // std::this_thread::sleep_for(std::chrono::microseconds());
      usleep(500);
      // std::this_thread::yield();
      continue;
    }

    if (lStfToSend > 1 || lStfToSend < 0) {
      LOG(WARNING) << "Data producer running slow. StfBacklog: " << lStfToSend;
    }

    const std::int64_t lPagesToSend = std::max(lStfToSend, int64_t(lStfToSend * (cStfLinkSize + cSuperpageSize - 1) / cSuperpageSize));

    // request enough superpages (can be less!)
    std::int64_t lPagesAvail = lSuperpages.size();
    if (lPagesAvail < lPagesToSend)
      lPagesAvail = mMemHandler->getSuperpages(std::max(lPagesToSend, std::int64_t(32)), std::back_inserter(lSuperpages));

    for (int64_t stf = 0; stf < lStfToSend; stf++, lSentStf++) {
      auto lHbfToSend = 256;

      while (lHbfToSend > 0) {
        if (!lSuperpages.empty()) {
          CRUSuperpage sp{ std::move(lSuperpages.front()) };
          lSuperpages.pop_front();

          // Enumerate valid data and create work-item for STFBuilder
          // Each channel is reported separately to the O2
          ReadoutLinkO2Data linkO2Data;

          linkO2Data.mLinkDataHeader.dataOrigin = (rand() % 100 < 70) ? o2::header::gDataOriginTPC : o2::header::gDataOriginITS;
          linkO2Data.mLinkDataHeader.dataDescription = o2::header::gDataDescriptionRawData;
          linkO2Data.mLinkDataHeader.payloadSerializationMethod = o2::header::gSerializationMethodNone;
          linkO2Data.mLinkDataHeader.subSpecification = mLinkID;

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

            linkO2Data.mLinkRawData.emplace_back(CruDmaPacket{
              mMemHandler->getDataRegion(),
              sp.mDataVirtualAddress + (d * mDmaChunkSize),        // Valid data DMA Chunk <superpage offset + length>
              mDmaChunkSize /* - (rand() % (mDmaChunkSize / 10))*/ // This should be taken from desc->mRawDataSize (filled by the CRU)
            });
          }

          // record how many chunks are there in a superpage
          linkO2Data.mLinkDataHeader.payloadSize = linkO2Data.mLinkRawData.size();
          // Put the link info data into the send queue
          mMemHandler->putLinkData(std::move(linkO2Data));

        } else {
          // signal lost data (no free superpages)
          ReadoutLinkO2Data linkO2Data;
          linkO2Data.mLinkDataHeader.subSpecification = -1;

          mMemHandler->putLinkData(std::move(linkO2Data));
          break;
        }
      }
    }
  }

  LOG(INFO) << "Exiting ReadoutEmulator thread...";
}

/// Start "data taking" thread
void CruLinkEmulator::start()
{
  mRunning = true;
  mCRULinkThread = std::thread(&CruLinkEmulator::linkReadoutThread, this);
}

/// Stop "data taking" thread
void CruLinkEmulator::stop()
{
  mRunning = false;
  if (mCRULinkThread.joinable()) {
    mCRULinkThread.join();
  }
}

}
} /* namespace o2::DataDistribution */
