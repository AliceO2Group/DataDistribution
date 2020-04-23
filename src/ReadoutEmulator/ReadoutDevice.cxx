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

  if (mSuperpageSize < (1ULL << 19)) {
    DDLOG(fair::Severity::WARN) << "Superpage size too low (" << mSuperpageSize << " Setting to 512kiB...";
    mSuperpageSize = (1ULL << 19);
  }

  mDmaChunkSize = (mCruLinkBitsPerS / 11223ULL) >> 3;
  DDLOG(fair::Severity::INFO) << "Using HBFrame size of " << mDmaChunkSize;

  mDataRegion.reset();

  // Open SHM regions (segments)
  mDataRegion = NewUnmanagedRegionFor(
    mOutChannelName, 0,
    mDataRegionSize,
    [this](void* data, size_t size, void* /* hint */) { // callback to be called when message buffers no longer needed by transport
      mCruMemoryHandler->put_data_buffer(static_cast<char*>(data), size);
    });

  DDLOG(fair::Severity::INFO) << "Memory regions created";

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
  mInfoThread = std::thread(&ReadoutDevice::InfoThread, this);

  // output thread
  mSendingThread = std::thread(&ReadoutDevice::SendingThread, this);
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
  static uint64_t lNumberSentStfs = 0;
  static uint64_t lCurrentTfId = 0;

  while (IsRunningState()) {

    auto isStfFinished =
      (std::chrono::high_resolution_clock::now() - cDataTakingStart) - (lNumberSentStfs * cStfInterval) > cStfInterval;

    if (isStfFinished) {
      lNumberSentStfs += 1;
      lCurrentTfId = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count() / cStfInterval.count();
    }

    ReadoutLinkO2Data lCruLinkData;
    if (!mCruMemoryHandler->getLinkData(lCruLinkData)) {
      DDLOG(fair::Severity::INFO) << "GetLinkData failed. Stopping interface thread.";
      return;
    }

    mFreeSuperpagesSamples.Fill(mCruMemoryHandler->free_superpages());

    // check no data signal
    if (lCruLinkData.mLinkDataHeader.subSpecification ==
      o2::header::DataHeader::SubSpecificationType(-1)) {
      // DDLOG(fair::Severity::WARN) << "No Superpages left! Losing data...";
    }

    ReadoutSubTimeframeHeader lHBFHeader;
    lHBFHeader.mTimeFrameId = lCurrentTfId;
    lHBFHeader.mNumberHbf = lCruLinkData.mLinkRawData.size();
    lHBFHeader.mLinkId = lCruLinkData.mLinkDataHeader.subSpecification;

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

  DDLOGF(fair::Severity::trace, "Exiting Info thread...");
}

}
} /* namespace o2::DataDistribution */
