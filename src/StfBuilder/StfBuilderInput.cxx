// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

#include "StfBuilderInput.h"
#include "StfBuilderDevice.h"

#include <SubTimeFrameBuilder.h>
#include <Utilities.h>

#include <DataDistLogger.h>

#include <FairMQDevice.h>

#include <vector>
#include <queue>
#include <chrono>
#include <sstream>

namespace o2
{
namespace DataDistribution
{

void StfInputInterface::start(const std::size_t pNumBuilders)
{
  mNumBuilders = pNumBuilders;
  mRunning = true;

  mBuilderInputQueues.clear();
  mBuilderInputQueues.resize(mNumBuilders);

  // NOTE: create the mStfBuilders first to avid resizing the vector; then threads
  for (std::size_t i = 0; i < mNumBuilders; i++) {
    mStfBuilders.emplace_back(mDevice.MemI(), mDevice.dplEnabled());
  }

  for (std::size_t i = 0; i < mNumBuilders; i++) {
    mBuilderThreads.emplace_back(create_thread_member("stfb_builder", &StfInputInterface::StfBuilderThread, this, i));
  }

  mInputThread = create_thread_member("stfb_input", &StfInputInterface::DataHandlerThread, this, 0);
}

void StfInputInterface::stop()
{
mRunning = false;

  for (auto &lBuilder : mStfBuilders) {
    lBuilder.stop();
  }

  if (mInputThread.joinable()) {
    mInputThread.join();
  }

  for (auto &lQueue : mBuilderInputQueues) {
    lQueue.stop();
  }

  for (auto &lBldThread : mBuilderThreads) {
    if (lBldThread.joinable()) {
      lBldThread.join();
    }
  }

  // mStfBuilders.clear(); // TODO: deal with shm region cleanup
  mBuilderThreads.clear();
  mBuilderInputQueues.clear();

  DDLOGF(fair::Severity::trace, "INPUT INTERFACE: Stopped.");
}

/// Receiving thread
void StfInputInterface::DataHandlerThread(const unsigned pInputChannelIdx)
{
  constexpr std::uint32_t cInvalidStfId = ~0;
  std::vector<FairMQMessagePtr> lReadoutMsgs;
  lReadoutMsgs.reserve(1024);
  // current TF Id
  std::uint32_t lCurrentStfId = cInvalidStfId;

  // Reference to the input channel
  auto& lInputChan = mDevice.GetChannel(mDevice.getInputChannelName(), pInputChannelIdx);

  try {
    while (mRunning) {

      // Equipment ID for the HBFrames (from the header)
      ReadoutSubTimeframeHeader lReadoutHdr;
      lReadoutMsgs.clear();

      // receive readout messages
      const auto lRet = lInputChan.Receive(lReadoutMsgs);
      if (lRet < 0 && mRunning) {
        DDLOGF_RL(1000, fair::Severity::WARNING, "READOUT INTERFACE: Receive failed . err={}", std::to_string(lRet));
        continue;
      } else if (lRet < 0 && !mRunning) {
        break; // should exit
      }

      if (lReadoutMsgs.empty()) {
        // nothing received?
        continue;
      }

      // Copy to avoid surprises. The receiving header is not O2 compatible and can be discarded
      if (lReadoutMsgs[0]->GetSize() != sizeof(ReadoutSubTimeframeHeader)) {
        DDLOGF_RL(1000, fair::Severity::ERROR,
          "READOUT INTERFACE: incompatible readout header received. Make sure to use compatible readout.exe version."
          " received_size={} expected_size={}", lReadoutMsgs[0]->GetSize(), sizeof(ReadoutSubTimeframeHeader));
        continue;
      }
      std::memcpy(&lReadoutHdr, lReadoutMsgs[0]->GetData(), sizeof(ReadoutSubTimeframeHeader));

      // check the readout header version
      if (lReadoutHdr.mVersion != sReadoutInterfaceVersion) {
        DDLOGF_RL(1000, fair::Severity::ERROR, "READOUT INTERFACE: Unsupported readout interface version. "
          "Make sure to use compatible readout.exe version. "
          "received={} expected={}", lReadoutHdr.mVersion, sReadoutInterfaceVersion);
        continue;
      }

      // check for backward/forward tf jumps
      if (lCurrentStfId != cInvalidStfId) {
        static thread_local std::uint64_t sNumNonContIncStfs = 0;
        static thread_local std::uint64_t sNumNonContDecStfs = 0;

        // backward jump
        if (lReadoutHdr.mTimeFrameId < lCurrentStfId) {
          std::stringstream lErrMsg;
          lErrMsg << "READOUT INTERFACE: "
              "TF ID decreased! (" << lCurrentStfId << ") -> (" << lReadoutHdr.mTimeFrameId << ") "
              "readout.exe sent messages with non-monotonic TF id! SubTimeFrames will be incomplete! "
              "Total occurrences: " << sNumNonContIncStfs;

          DDLOGF_RL(200, fair::Severity::ERROR, lErrMsg.str());
          DDLOGF(fair::Severity::DEBUG, lErrMsg.str());

          // TODO: accout for lost data
          continue;
        }

        // forward jump
        if (lReadoutHdr.mTimeFrameId > (lCurrentStfId + 1)) {
          DDLOGF_RL(200, fair::Severity::WARNING, "READOUT INTERFACE: "
            "TF ID non-contiguous increase! ({}) -> ({}). Total occurrences: {}", lCurrentStfId,
            lReadoutHdr.mTimeFrameId, sNumNonContDecStfs);
        }
        // we keep the data since this might be a legitimate jump
      }

      // get the current TF id
      lCurrentStfId = lReadoutHdr.mTimeFrameId;

      mBuilderInputQueues[lReadoutHdr.mTimeFrameId % mNumBuilders].push(std::move(lReadoutMsgs));
    }
  } catch (std::runtime_error& e) {
    DDLOGF(fair::Severity::ERROR, "Input channel receive failed. Stopping input thread...");
    return;
  }

  DDLOGF(fair::Severity::trace, "Exiting the input thread...");
}

/// StfBuilding thread
void StfInputInterface::StfBuilderThread(const std::size_t pIdx)
{
  static constexpr bool cBuildOnTimeout = false;
  using namespace std::chrono_literals;
  // current TF Id
  constexpr std::uint32_t cInvalidStfId = ~0;
  std::uint32_t lCurrentStfId = cInvalidStfId;
  bool lStarted = false;
  std::vector<FairMQMessagePtr> lReadoutMsgs;
  lReadoutMsgs.reserve(1U << 20);

  // Reference to the input channel
  assert (mBuilderInputQueues.size() == mNumBuilders);
  assert (pIdx < mBuilderInputQueues.size());
  auto &lInputQueue = mBuilderInputQueues[pIdx];

  // Stf builder
  SubTimeFrameReadoutBuilder &lStfBuilder = mStfBuilders[pIdx];

  const std::chrono::microseconds cMinWaitTime = 2s;
  const std::chrono::microseconds cDesiredWaitTime = 2s * mNumBuilders / 3;
  const auto cStfDataWaitFor = std::max(cMinWaitTime, cDesiredWaitTime);

  using hres_clock = std::chrono::high_resolution_clock;
  auto lStfStartTime = hres_clock::now();

    while (mRunning) {

      auto finishBuildingCurrentStf = [&](bool pTimeout = false) {
        // Finished: queue the current STF and start a new one
        ReadoutDataUtils::sFirstSeenHBOrbitCnt = 0;

        if (auto lStf = lStfBuilder.getStf()) {
          // start the new STF
          if (pTimeout) {
            DDLOGF(fair::Severity::WARNING, "READOUT INTERFACE: finishing STF on a timeout. stf_id={} size={}",
              (*lStf)->header().mId, (*lStf)->getDataSize());
          }

          mDevice.queue(eStfBuilderOut, std::move(*lStf));

          { // MON: data of a new STF received, get the freq and new start time
            const auto lStfDur = std::chrono::duration<float>(hres_clock::now() - lStfStartTime);
            mStfFreqSamples.Fill(1.0f / lStfDur.count() * mNumBuilders);
            lStfStartTime = hres_clock::now();
          }
        }
      };

      // Equipment ID for the HBFrames (from the header)
      lReadoutMsgs.clear();

      // receive readout messages
      const auto lRet = lInputQueue.pop_wait_for(lReadoutMsgs, cStfDataWaitFor);
      if (!lRet && mRunning) {
        if (lStarted) {
          // finish on a timeout
          finishBuildingCurrentStf(cBuildOnTimeout);
        }
        continue;
      } else if (!mRunning) {
        break;
      }

      // must not be empty
      if (lReadoutMsgs.empty()) {
        DDLOGF(fair::Severity::ERROR, "READOUT INTERFACE: empty readout multipart.");
        continue;
      }

      // stated to build STFs
      lStarted = true;

      // Copy to avoid surprises. The receiving header is not O2 compatible and can be discarded
      ReadoutSubTimeframeHeader lReadoutHdr;
      // the size is checked on receive
      std::memcpy(&lReadoutHdr, lReadoutMsgs[0]->GetData(), sizeof(ReadoutSubTimeframeHeader));

      // log only
      DDLOGF_RL(1000, fair::Severity::DEBUG, "READOUT INTERFACE: Received an ReadoutMsg. stf_id={}",
        lReadoutHdr.mTimeFrameId);

      // check multipart size
      if (lReadoutMsgs.size() == 1 && !lReadoutHdr.mFlags.mLastTFMessage) {
        DDLOGF_RL(1000, fair::Severity::ERROR,
          "READOUT INTERFACE: Received only a header message without the STF stop bit set.");
        continue;
      }

      // check the link/feeids (first HBF only)
      if (lReadoutMsgs.size() > 1 && lReadoutHdr.mFlags.mIsRdhFormat) {
        try {
          const auto R = RDHReader(lReadoutMsgs[1]);
          const auto lLinkId = R.getLinkID();

          if (lLinkId != lReadoutHdr.mLinkId) {
            DDLOGF(fair::Severity::ERROR, "READOUT INTERFACE: Update link ID does not match RDH in the data block."
              " hdr_link_id={} rdh_link_id={}", lReadoutHdr.mLinkId, lLinkId);
          }
        } catch (RDHReaderException &e) {
          DDLOGF(fair::Severity::ERROR, e.what());
          // TODO: the whole ReadoutMsg is discarded. Account and report the data size.
          continue;
        }
      }

      const auto lIdInBuilding = lStfBuilder.getCurrentStfId();
      lCurrentStfId = lIdInBuilding ? *lIdInBuilding : lReadoutHdr.mTimeFrameId;

      // check for the new TF marker
      if (lReadoutHdr.mTimeFrameId != lCurrentStfId) {
        // we expect to be notified about new TFs
        if (lIdInBuilding) {
          DDLOGF_RL(1000, fair::Severity::ERROR, "READOUT INTERFACE: Update with a new TF ID. Stop flag not received "
            "the current STF. current_id={} new_id={} ", lCurrentStfId, lReadoutHdr.mTimeFrameId);
          finishBuildingCurrentStf();
        }
        lCurrentStfId = lReadoutHdr.mTimeFrameId;
      }

      // check subspecifications of all messages
      header::DataHeader::SubSpecificationType lSubSpecification = ~header::DataHeader::SubSpecificationType(0);
      header::DataOrigin lDataOrigin;
      try {
        const auto R1 = RDHReader(lReadoutMsgs[1]);
        lDataOrigin = ReadoutDataUtils::getDataOrigin(R1);
        lSubSpecification = ReadoutDataUtils::getSubSpecification(R1);
      } catch (RDHReaderException &e) {
        DDLOGF(fair::Severity::ERROR, "READOUT_INTERFACE: Cannot parse RDH of received HBFs. what={}", e.what());
        // TODO: the whole ReadoutMsg is discarded. Account and report the data size.
        continue;
      }

      assert (lReadoutMsgs.size() > 1);
      auto lStartHbf = lReadoutMsgs.begin() + 1; // skip the meta message
      auto lEndHbf = lStartHbf + 1;

      std::size_t lAdded = 0;
      bool lErrorWhileAdding = false;
      const bool lFinishStf = lReadoutHdr.mFlags.mLastTFMessage;

      while (true) {
        if (lEndHbf == lReadoutMsgs.end()) {
          //insert the remaining span
          std::size_t lInsertCnt = (lEndHbf - lStartHbf);
          lStfBuilder.addHbFrames(lDataOrigin, lSubSpecification, lReadoutHdr, lStartHbf, lInsertCnt);
          lAdded += lInsertCnt;
          break;
        }

        header::DataHeader::SubSpecificationType lNewSubSpec = ~header::DataHeader::SubSpecificationType(0);
        try {
          const auto Rend = RDHReader(*lEndHbf);
          lNewSubSpec = ReadoutDataUtils::getSubSpecification(Rend);
        } catch (RDHReaderException &e) {
          DDLOGF(fair::Severity::ERROR, e.what());
          // TODO: portion of the ReadoutMsg is discarded. Account and report the data size.
          lErrorWhileAdding = true;
          break;
        }

        if (lNewSubSpec != lSubSpecification) {
          DDLOGF(fair::Severity::ERROR, "READOUT INTERFACE: update with mismatched subspecification."
            " block[0]: {:#06x}, block[{}]: {:#06x}",
            lSubSpecification, (lEndHbf - (lReadoutMsgs.begin() + 1)), lNewSubSpec);
          // insert
          lStfBuilder.addHbFrames(lDataOrigin, lSubSpecification, lReadoutHdr, lStartHbf, lEndHbf - lStartHbf);
          lAdded += (lEndHbf - lStartHbf);
          lStartHbf = lEndHbf;

          lSubSpecification = lNewSubSpec;
        }
        lEndHbf = lEndHbf + 1;
      }

      if (!lErrorWhileAdding && (lAdded != lReadoutMsgs.size() - 1) ) {
        DDLOGF(fair::Severity::ERROR, "BUG: Not all received HBFrames added to the STF...");
      }

      // check if this was the last message of an STF
      if (lFinishStf) {
        finishBuildingCurrentStf();
      }
    }

  DDLOGF(fair::Severity::trace, "Exiting StfBuilder thread...");
}

}
}
