// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "ReadoutDevice.h"
#include "DataDistLogger.h"

#include <ReadoutDataModel.h>

#include <MemoryUtils.h>

#include <options/FairMQProgOptions.h>

#include <chrono>
#include <thread>
#include <functional>


namespace o2::DataDistribution
{

ReadoutDevice::ReadoutDevice()
  : DataDistDevice(),
    mCruMemoryHandler{ std::make_shared<CruMemoryHandler>() }
{
  mDataBlockMsgs.reserve(1024);
}

ReadoutDevice::~ReadoutDevice()
{
}

void ReadoutDevice::InitTask()
{
  mOutChannelName = GetConfig()->GetValue<std::string>(OptionKeyOutputChannelName);
  mDataRegionSize = GetConfig()->GetValue<std::size_t>(OptionKeyReadoutDataRegionSize);
  mLinkIdOffset = GetConfig()->GetValue<std::size_t>(OptionKeyLinkIdOffset);
  mSuperpageSize = GetConfig()->GetValue<std::size_t>(OptionKeyCruSuperpageSize);
  mOrbitsInTf = GetConfig()->GetValue<std::size_t>(OptionKeyOrbitsInTf);
  mCruLinkCount = GetConfig()->GetValue<std::size_t>(OptionKeyCruLinkCount);
  mCruLinkBitsPerS = GetConfig()->GetValue<double>(OptionKeyCruLinkBitsPerS);

  if (mSuperpageSize < (1ULL << 20)) {
    WDDLOG("Superpage size too low ({}). Setting to 1 MiB...", mSuperpageSize);
    mSuperpageSize = (1ULL << 20);
  }

  mDmaChunkSize = (mCruLinkBitsPerS / 11223ULL) >> 3;
  IDDLOG("Using HBFrame size of {} B.", mDmaChunkSize);

  // mDataRegion.stop();

  // Open SHM regions (segments). Increase size to make sure we can start on the mSuperpageSize boundary
  std::string lSegName = "cru_seg";
  mDataRegion = std::make_unique<DataRegionAllocatorResource>(lSegName, std::nullopt, mDataRegionSize + mSuperpageSize, *Transport(), 0, true);

  IDDLOG("Memory regions created");

  mCruMemoryHandler->init(mDataRegion.get(), mSuperpageSize);

  mCruLinks.clear();
  for (unsigned e = 0; e < mCruLinkCount; e++) {
    mCruLinks.push_back(std::make_unique<CruLinkEmulator>(mCruMemoryHandler, mLinkIdOffset + e,
      mCruLinkBitsPerS, mDmaChunkSize, mOrbitsInTf));
  }
}

void ReadoutDevice::PreRun()
{
  // start all cru link emulators
  for (auto& e : mCruLinks)
    e->start();

  // info thread
  mInfoThread = create_thread_member("readout_info", &ReadoutDevice::InfoThread, this);

  // output thread
  mSendingThread = create_thread_member("readout_out", &ReadoutDevice::SendingThread, this);
}

void ReadoutDevice::ResetTask()
{
  // stop all cru link emulators
  for (auto& e : mCruLinks)
    e->stop();
  // unblock waiters
  mCruMemoryHandler->teardown();

  if (mInfoThread.joinable()) {
    mInfoThread.join();
  }

  if (mSendingThread.joinable()) {
    mSendingThread.join();
  }

  mDataRegion = nullptr;
}

bool ReadoutDevice::ConditionalRun()
{
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(1s);
  return true;
}

void ReadoutDevice::SendingThread()
{

  WaitForRunningState();

  auto& lOutputChan = GetChannel(mOutChannelName, 0);

  // finish an STF every ~1/45 seconds
  static const auto cDataTakingStart = std::chrono::high_resolution_clock::now();
  static const auto cStfInterval = std::chrono::microseconds((1000000ULL * mOrbitsInTf) / 11223ULL);
  uint64_t lNumberSentStfs = 0;
  uint64_t lCurrentTfId = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()).count() / cStfInterval.count() - 71394000000;

  uint64_t lTfIdToSkip = ~uint64_t(0);


  while (IsRunningState()) {

    ReadoutLinkO2Data lCruLinkData;
    if (!mCruMemoryHandler->getLinkData(lCruLinkData)) {
      IDDLOG("GetLinkData failed. Stopping interface thread.");
      return;
    }

    // check no data signal
    if (lCruLinkData.mLinkHeader.mFlags.mIsRdhFormat == 0) {
      // WDDLOG("No Superpages left! Losing data...");
    }

    // debug: skip some of the STFs
    if (lTfIdToSkip == ~uint64_t(0)) {
      if ((std::hash<unsigned long long>{}((unsigned long long)lCurrentTfId) % 67) == 0 ) {
        lTfIdToSkip = lCurrentTfId + 1;
        IDDLOG("Skipping sending data for tf_id={}", lTfIdToSkip);
      }
    } else if (lTfIdToSkip != ~uint64_t(0)) {
      if (lTfIdToSkip < lCurrentTfId) {
        lTfIdToSkip = ~uint64_t(0);
      }
    }

    ReadoutSubTimeframeHeader lHBFHeader = lCruLinkData.mLinkHeader;
    lHBFHeader.mVersion = 2;
    lHBFHeader.mTimeFrameId = lCurrentTfId;
    lHBFHeader.mTimeframeOrbitFirst = lCurrentTfId * 256;
    lHBFHeader.mTimeframeOrbitLast = (lCurrentTfId + 1) * 256 - 1;
    lHBFHeader.mFlags.mLastTFMessage = 0;

    // last one?
    auto isStfFinished =
      (std::chrono::high_resolution_clock::now() - cDataTakingStart) - (lNumberSentStfs * cStfInterval) > cStfInterval;

    if (isStfFinished) {
      lNumberSentStfs += 1;
      lCurrentTfId = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()).count() / cStfInterval.count() - 71394000000;
      lHBFHeader.mFlags.mLastTFMessage = 1;
    }

    assert(mDataBlockMsgs.empty());
    mDataBlockMsgs.reserve(lCruLinkData.mLinkRawData.size());

    // create messages for the header
    mDataBlockMsgs.push_back(lOutputChan.NewMessage(sizeof(ReadoutSubTimeframeHeader)));
    std::memcpy(mDataBlockMsgs.front()->GetData(), &lHBFHeader, sizeof(ReadoutSubTimeframeHeader));

    // create messages for the data
    for (const auto& lDmaChunk : lCruLinkData.mLinkRawData) {
      // create a message out of unmanaged region
      mDataBlockMsgs.push_back(mDataRegion->NewFairMQMessageFromPtr(lDmaChunk.mDataPtr, lDmaChunk.mDataSize));
    }

    if (lTfIdToSkip == lHBFHeader.mTimeFrameId) {
      mDataBlockMsgs.clear();
      continue;
    }

    lOutputChan.Send(mDataBlockMsgs);
    mDataBlockMsgs.clear();
  }
}

void ReadoutDevice::InfoThread()
{
  WaitForRunningState();

  while (IsRunningState()) {

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(2s);
  }

  DDDLOG("Exiting Info thread...");
}

} /* namespace o2::DataDistribution */
