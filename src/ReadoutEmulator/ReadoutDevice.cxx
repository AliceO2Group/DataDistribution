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

#include <options/FairMQProgOptions.h>

#include <chrono>
#include <thread>

namespace o2
{
namespace DataDistribution
{

ReadoutDevice::ReadoutDevice()
  : DataDistDevice(),
    mCruMemoryHandler{ std::make_shared<CruMemoryHandler>() },
    mFreeSuperpagesSamples()
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

  mCruLinkCount = GetConfig()->GetValue<std::size_t>(OptionKeyCruLinkCount);
  mCruLinkBitsPerS = GetConfig()->GetValue<double>(OptionKeyCruLinkBitsPerS);

  if (mSuperpageSize < (1ULL << 20)) {
    DDLOGF(fair::Severity::WARN, "Superpage size too low ({}). Setting to 1 MiB...", mSuperpageSize);
    mSuperpageSize = (1ULL << 20);
  }

  mDmaChunkSize = (mCruLinkBitsPerS / 11223ULL) >> 3;
  DDLOGF(fair::Severity::INFO, "Using HBFrame size of {} B.", mDmaChunkSize);

  mDataRegion.reset();

  // Open SHM regions (segments). Increase size to make sure we can start on the mSuperpageSize boundary
  mDataRegion = NewUnmanagedRegionFor(
    mOutChannelName, 0,
    mDataRegionSize + mSuperpageSize,
    [this](const std::vector<FairMQRegionBlock>& pBlkVect) { // callback to be called when message buffers no longer needed by transport
      for (const auto &lBlk : pBlkVect) {
          mCruMemoryHandler->put_data_buffer(static_cast<char*>(lBlk.ptr), lBlk.size);
      }
    });

  DDLOGF(fair::Severity::INFO, "Memory regions created");

  mCruMemoryHandler->init(mDataRegion.get(), mSuperpageSize);

  mCruLinks.clear();
  for (unsigned e = 0; e < mCruLinkCount; e++)
    mCruLinks.push_back(std::make_unique<CruLinkEmulator>(mCruMemoryHandler, mLinkIdOffset + e, mCruLinkBitsPerS, mDmaChunkSize));
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
  static constexpr auto cStfInterval = std::chrono::microseconds(22810);
  uint64_t lNumberSentStfs = 0;
  uint64_t lCurrentTfId = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()).count() / cStfInterval.count();;

  while (IsRunningState()) {

    ReadoutLinkO2Data lCruLinkData;
    if (!mCruMemoryHandler->getLinkData(lCruLinkData)) {
      DDLOGF(fair::Severity::INFO, "GetLinkData failed. Stopping interface thread.");
      return;
    }

    mFreeSuperpagesSamples.Fill(mCruMemoryHandler->free_superpages());

    // check no data signal
    if (lCruLinkData.mLinkHeader.mFlags.mIsRdhFormat == 0) {
      // DDLOGF(fair::Severity::WARN, "No Superpages left! Losing data...");
    }

    ReadoutSubTimeframeHeader lHBFHeader = lCruLinkData.mLinkHeader;
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
        std::chrono::high_resolution_clock::now().time_since_epoch()).count() / cStfInterval.count();
      lHBFHeader.mFlags.mLastTFMessage = 1;
    }

    assert(mDataBlockMsgs.empty());
    mDataBlockMsgs.reserve(lCruLinkData.mLinkRawData.size());

    // create messages for the header
    mDataBlockMsgs.emplace_back(lOutputChan.NewMessage(sizeof(ReadoutSubTimeframeHeader)));
    std::memcpy(mDataBlockMsgs.front()->GetData(), &lHBFHeader, sizeof(ReadoutSubTimeframeHeader));

    // create messages for the data
    for (const auto& lDmaChunk : lCruLinkData.mLinkRawData) {
      // mark this as used in the memory handler
      mCruMemoryHandler->get_data_buffer(lDmaChunk.mDataPtr, lDmaChunk.mDataSize);

      // create a message out of unmanaged region
      mDataBlockMsgs.emplace_back(lOutputChan.NewMessage(mDataRegion, lDmaChunk.mDataPtr, lDmaChunk.mDataSize));
    }

    lOutputChan.Send(mDataBlockMsgs);
    mDataBlockMsgs.clear();
  }
}

void ReadoutDevice::InfoThread()
{
  WaitForRunningState();

  while (IsRunningState()) {
    DDLOGF(fair::Severity::info, "Free superpages count_mean={}", mFreeSuperpagesSamples.Mean());

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(2s);
  }

  DDLOGF(fair::Severity::DEBUG, "Exiting Info thread...");
}

}
} /* namespace o2::DataDistribution */
