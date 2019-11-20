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

#include <FairMQDevice.h>
#include <FairMQLogger.h>

#include <vector>
#include <queue>
#include <chrono>

namespace o2
{
namespace DataDistribution
{

void StfInputInterface::start(const std::size_t pNumBuilders, const o2::header::DataOrigin &pDataOrig)
{
  mNumBuilders = pNumBuilders;
  mDataOrigin = pDataOrig;
  mRunning = true;

  mBuilderInputQueues.clear();
  mBuilderInputQueues.resize(mNumBuilders);

  // Reference to the output or DPL channel
  const auto &lOutChanName = mDevice.dplEnabled() ?
    mDevice.getDplChannelName() :
    mDevice.getOutputChannelName();

  auto& lOutputChan = mDevice.GetChannel(lOutChanName);

  // NOTE: create the mStfBuilders first to avid resizing the vector; then threads
  for (std::size_t i = 0; i < mNumBuilders; i++) {
    mStfBuilders.emplace_back(lOutputChan, mDevice.dplEnabled());
  }

  for (std::size_t i = 0; i < mNumBuilders; i++) {
    mBuilderThreads.emplace_back(std::thread(&StfInputInterface::StfBuilderThread, this, i));
  }

  mInputThread = std::thread(&StfInputInterface::DataHandlerThread, this, 0);
}

void StfInputInterface::stop()
{
  mRunning = false;
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

  mStfBuilders.clear();
  mBuilderThreads.clear();
  mBuilderInputQueues.clear();
}

/// Receiving thread
void StfInputInterface::DataHandlerThread(const unsigned pInputChannelIdx)
{
  std::vector<FairMQMessagePtr> lReadoutMsgs;
  lReadoutMsgs.reserve(1U << 20);
  // current TF Id
  std::uint64_t lCurrentStfId = 0;

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
        // LOG(WARNING) << "StfHeader receive failed (err = " + std::to_string(lRet) + ")";
        // std::this_thread::yield();
        continue;
      } else if (lRet < 0) {
        break; // should exit?
      }

      if (lReadoutMsgs.empty() ) {
        // nothing received?
        continue;
      }

      // Copy to avoid surprises. The receiving header is not O2 compatible and can be discarded
      assert(lReadoutMsgs[0]->GetSize() == sizeof(ReadoutSubTimeframeHeader));
      std::memcpy(&lReadoutHdr, lReadoutMsgs[0]->GetData(), sizeof(ReadoutSubTimeframeHeader));

      {
        static thread_local std::uint64_t sNumContIncProblems = 0;
        static thread_local std::uint64_t sNumContDecProblems = 0;

        if (lReadoutHdr.mTimeFrameId < lCurrentStfId) {
          if (sNumContIncProblems++ % 50 == 0) {
            LOG(ERROR) << "READOUT INTERFACE: "
              "TF ID decreased! (" << lCurrentStfId << ") -> (" << lReadoutHdr.mTimeFrameId << ") "
              "readout.exe sent messages with non-monotonic TF id! SubTimeFrames will be incomplete! "
              "Total occurrences: " << sNumContIncProblems;
          }
        }

        if (lReadoutHdr.mTimeFrameId > (lCurrentStfId + 1)) {
          if (sNumContDecProblems++ % 50 == 0) {
            LOG(ERROR) << "READOUT INTERFACE: "
              "TF ID non-contiguous increase! (" << lCurrentStfId << ") -> (" << lReadoutHdr.mTimeFrameId << ") "
              "readout.exe sent messages with non-monotonic TF id! SubTimeFrames will be incomplete! "
              "Total occurrences: " << sNumContDecProblems;
          }
        }
      }

      lCurrentStfId = std::max(lCurrentStfId, std::uint64_t(lReadoutHdr.mTimeFrameId));

      mBuilderInputQueues[lReadoutHdr.mTimeFrameId % mNumBuilders].push(std::move(lReadoutMsgs));
    }
  } catch (std::runtime_error& e) {
    LOG(ERROR) << "Receive failed. Stopping input thread[" << pInputChannelIdx << "]...";
    return;
  }

  LOG(INFO) << "Exiting input thread[" << pInputChannelIdx << "]...";
}

/// StfBuilding thread
void StfInputInterface::StfBuilderThread(const std::size_t pIdx)
{
  using namespace std::chrono_literals;
  // current TF Id
  std::int64_t lCurrentStfId = 0;
  std::vector<FairMQMessagePtr> lReadoutMsgs;
  lReadoutMsgs.reserve(1U << 20);

  // Reference to the input channel
  assert (mBuilderInputQueues.size() == mNumBuilders);
  assert (pIdx < mBuilderInputQueues.size());
  auto &lInputQueue = mBuilderInputQueues[pIdx];

  // Stf builder
  SubTimeFrameReadoutBuilder &lStfBuilder = mStfBuilders[pIdx];
  lStfBuilder.setRdh4FilterTrigger(mRdh4FilterTrigger);

  const std::chrono::microseconds cMinWaitTime = 20000us;
  const std::chrono::microseconds cDesiredWaitTime = 22500us * mNumBuilders / 3;
  const auto cStfDataWaitFor = std::max(cMinWaitTime, cDesiredWaitTime);

  using hres_clock = std::chrono::high_resolution_clock;
  auto lStfStartTime = hres_clock::now();

    while (mRunning) {

      bool lFinishStf = false;

      // Equipment ID for the HBFrames (from the header)
      lReadoutMsgs.clear();

      // receive readout messages
      const auto lRet = lInputQueue.pop_wait_for(lReadoutMsgs, cStfDataWaitFor);
      if (!lRet && mRunning) {
        lFinishStf = true;
        assert (lReadoutMsgs.empty());
      } else if (!mRunning) {
        break;
      }

      if (lFinishStf) {
        std::unique_ptr<SubTimeFrame> lStf = lStfBuilder.getStf();
        if (lStf) {
          mDevice.queue(eStfBuilderOut, std::move(lStf));

          { // MON: data of a new STF received, get the freq and new start time
            if (mDevice.guiEnabled()) {
              const auto lStfDur = std::chrono::duration<float>(hres_clock::now() - lStfStartTime);
              mStfFreqSamples.Fill(1.0f / lStfDur.count() * mNumBuilders);
              lStfStartTime = hres_clock::now();
            }
          }
        }

        continue;
      }

      if (lReadoutMsgs.empty()) {
        LOG(ERROR) << "READOUT INTERFACE: empty readout multipart.";
        continue;
      }

      // Copy to avoid surprises. The receiving header is not O2 compatible and can be discarded
      ReadoutSubTimeframeHeader lReadoutHdr;
      assert(lReadoutMsgs[0]->GetSize() == sizeof(ReadoutSubTimeframeHeader));
      std::memcpy(&lReadoutHdr, lReadoutMsgs[0]->GetData(), sizeof(ReadoutSubTimeframeHeader));

      if (lReadoutHdr.mTimeFrameId % (100 + pIdx) == 0) {
        static thread_local std::uint64_t sStfSeen = 0;
        if (lReadoutHdr.mTimeFrameId != sStfSeen) {
          sStfSeen = lReadoutHdr.mTimeFrameId;
          LOG(DEBUG) << "READOUT INTERFACE [" << pIdx << "]: "
            "Received an update for STF ID: " << lReadoutHdr.mTimeFrameId;
        }
      }

      // check multipart size
      {
        if (lReadoutHdr.mNumberHbf != (lReadoutMsgs.size() - 1)) {
          static thread_local std::uint64_t sNumMessages = 0;
          if (sNumMessages++ % 8192 == 0) {
            LOG(ERROR) << "READOUT INTERFACE [" << pIdx << "]: "
              "indicated number of HBFrames in the header does not match the number of sent blocks: "
                       << lReadoutHdr.mNumberHbf << " != " << (lReadoutMsgs.size() - 1)
                       << ". Total occurrences: " << sNumMessages;
          }

          lReadoutHdr.mNumberHbf = lReadoutMsgs.size() - 1;
        }

        if (lReadoutMsgs.size() > 1) {

          const auto [lCruId, lEndPoint, lLinkId] = ReadoutDataUtils::getSubSpecificationComponents(
            static_cast<const char*>(lReadoutMsgs[1]->GetData()),
            lReadoutMsgs[1]->GetSize());

          (void)lCruId;

          if (lLinkId != lReadoutHdr.mLinkId) {
            LOG(ERROR) << "READOUT INTERFACE [" << pIdx << "]: "
                          "indicated link ID does not match RDH in data block "
                       << (unsigned)lReadoutHdr.mLinkId << " != " << lLinkId;
          }
        }
      }

      if (lReadoutMsgs.size() < 2) {
        LOG(ERROR) << "READOUT INTERFACE [" << pIdx << "]: no data sent.";
        continue;
      }

      const auto lSubSpecification = ReadoutDataUtils::getSubSpecification(
        static_cast<const char*>(lReadoutMsgs[1]->GetData()),
        lReadoutMsgs[1]->GetSize()
      );

      // LOG(DEBUG) << "RECEIVED::Header::size: " << lReadoutMsgs[0]->GetSize() << ", "
      //           << "TF id: " << lReadoutHdr.mTimeFrameId << ", "
      //           << "#HBF: " << lReadoutHdr.mNumberHbf << ", "
      //           << "EQ: " << lReadoutHdr.linkId;

      // check for the new TF marker
      if (lReadoutHdr.mTimeFrameId != lCurrentStfId) {

        if (lReadoutMsgs.size() > 1) {
          ReadoutDataUtils::sFirstSeenHBOrbitCnt = 0;
        }

        if (lCurrentStfId >= 0) {
          // Finished: queue the current STF and start a new one
          std::unique_ptr<SubTimeFrame> lStf = lStfBuilder.getStf();
          if (lStf) {
            // LOG(DEBUG) << "Received TF[" << lStf->header().mId<< "]::size= " << lStf->getDataSize();
            mDevice.queue(eStfBuilderOut, std::move(lStf));

            { // MON: data of a new STF received, get the freq and new start time
              if (mDevice.guiEnabled()) {
                const auto lStfDur = std::chrono::duration<float>(hres_clock::now() - lStfStartTime);
                mStfFreqSamples.Fill(1.0f / lStfDur.count() * mNumBuilders);
                lStfStartTime = hres_clock::now();
              }
            }
          }
        }

        // start a new STF
        lCurrentStfId = lReadoutHdr.mTimeFrameId;
      }

      // handle HBFrames
      assert(lReadoutHdr.mNumberHbf > 0);
      assert(lReadoutHdr.mNumberHbf == lReadoutMsgs.size() - 1);

      lStfBuilder.addHbFrames(mDataOrigin, lSubSpecification, lReadoutHdr, std::move(lReadoutMsgs));
    }

  LOG(INFO) << "Exiting StfBuilder thread[" << pIdx << "]...";
}

}
}
