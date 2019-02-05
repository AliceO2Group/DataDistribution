// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "SubTimeFrameBuilderInput.h"
#include "SubTimeFrameBuilderDevice.h"
#include <SubTimeFrameVisitors.h>
#include <Utilities.h>

#include <O2Device/O2Device.h>
#include <FairMQDevice.h>
#include <FairMQStateMachine.h>
#include <FairMQLogger.h>

#include <vector>
#include <queue>
#include <chrono>

namespace o2
{
namespace DataDistribution
{

void StfInputInterface::Start()
{
  if (!mDevice.CheckCurrentState(StfBuilderDevice::RUNNING)) {
    LOG(WARN) << "Not creating interface threads. StfBuilder is not running.";
    return;
  }

  mInputThread = std::thread(&StfInputInterface::DataHandlerThread, this, 0);
}

void StfInputInterface::Stop()
{
  if (mInputThread.joinable()) {
    mInputThread.join();
  }
}

/// Receiving thread
void StfInputInterface::DataHandlerThread(const unsigned pInputChannelIdx)
{
  // current TF Id
  std::int64_t lCurrentStfId = -1;
  std::vector<FairMQMessagePtr> lReadoutMsgs;
  lReadoutMsgs.reserve(4 * 1024);

  // Reference to the input channel
  auto& lInputChan = mDevice.GetChannel(mDevice.getInputChannelName(), pInputChannelIdx);
  auto& lOutputChan = mDevice.GetChannel(mDevice.getOutputChannelName());

  // Stf builder
  SubTimeFrameReadoutBuilder lStfBuilder(lOutputChan);

  using hres_clock = std::chrono::high_resolution_clock;
  auto lStfStartTime = hres_clock::now();

  try {
    while (mDevice.CheckCurrentState(StfBuilderDevice::RUNNING)) {

      // Equipment ID for the HBFrames (from the header)
      ReadoutSubTimeframeHeader lReadoutHdr;

      assert(lReadoutMsgs.empty());

      // receive readout messages
      auto lRet = lInputChan.Receive(lReadoutMsgs);
      if (lRet < 0 && mDevice.CheckCurrentState(StfBuilderDevice::RUNNING)) {
        LOG(WARNING) << "StfHeader receive failed (err = " + std::to_string(lRet) + ")";
        lReadoutMsgs.clear();
        continue;
      } else if (!mDevice.CheckCurrentState(StfBuilderDevice::RUNNING)) {
        break; // should exit?
      }

      // Copy to avoid surprises. The receiving header is not O2 compatible and can be discarded
      assert(lReadoutMsgs[0]->GetSize() == sizeof(ReadoutSubTimeframeHeader));
      std::memcpy(&lReadoutHdr, lReadoutMsgs[0]->GetData(), sizeof(ReadoutSubTimeframeHeader));

      // LOG(DEBUG) << "RECEIVED::Header::size: " << lReadoutMsgs[0]->GetSize() << ", "
      //           << "TF id: " << lReadoutHdr.timeframeId << ", "
      //           << "#HBF: " << lReadoutHdr.numberOfHBF << ", "
      //           << "EQ: " << lReadoutHdr.linkId;

      // check for the new TF marker
      if (lReadoutHdr.timeframeId != lCurrentStfId) {

        // data of a new STF received, get the freq and new start time
        if (mDevice.guiEnabled()) {
          const auto lStfDur = std::chrono::duration<float>(hres_clock::now() - lStfStartTime);
          mStfFreqSamples.Fill(1.0f / lStfDur.count());
          lStfStartTime = hres_clock::now();
        }

        if (lCurrentStfId > 0 && lReadoutHdr.timeframeId < lCurrentStfId) {
          LOG(WARN) << "TF ID decreased! (" << lCurrentStfId << ") -> (" << lReadoutHdr.timeframeId << ")";
          // what now?
        }

        if (lCurrentStfId >= 0) {
          // Finished: queue the current STF
          std::unique_ptr<SubTimeFrame> lStf = lStfBuilder.getStf();
          if (lStf) {
            // LOG(DEBUG) << "Received TF[" << lStf->header().mId<< "]::size= " << lStf->getDataSize();
            mDevice.queue(eStfBuilderOut, std::move(lStf));
          } else {
            LOG(INFO) << "No data received? This should not happen.";
          }
        }

        // start a new STF
        lCurrentStfId = lReadoutHdr.timeframeId;
      }

      // handle HBFrames
      assert(lReadoutHdr.numberOfHBF > 0);
      assert(lReadoutHdr.numberOfHBF == lReadoutMsgs.size() - 1);

      lStfBuilder.addHbFrames(lReadoutHdr, std::move(lReadoutMsgs));

      lReadoutMsgs.clear();
    }
  } catch (std::runtime_error& e) {
    LOG(ERROR) << "Receive failed. Stopping input thread[" << pInputChannelIdx << "]...";
    return;
  }

  LOG(INFO) << "Exiting input thread[" << pInputChannelIdx << "]...";
}
}
}
