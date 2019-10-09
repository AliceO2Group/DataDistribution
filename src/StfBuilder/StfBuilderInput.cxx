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

void StfInputInterface::start(const o2::header::DataOrigin &pDataOrig)
{
  mDataOrigin = pDataOrig;
  mRunning = true;
  mInputThread = std::thread(&StfInputInterface::DataHandlerThread, this, 0);
}

void StfInputInterface::stop()
{
  mRunning = false;
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
  lReadoutMsgs.reserve(1U << 20);

  // Reference to the input channel
  auto& lInputChan = mDevice.GetChannel(mDevice.getInputChannelName(), pInputChannelIdx);

  // Reference to the output or DPL channel
  const auto &lOutChanName = mDevice.dplEnabled() ?
    mDevice.getDplChannelName() :
    mDevice.getOutputChannelName();

  auto& lOutputChan = mDevice.GetChannel(lOutChanName);

  // Stf builder
  SubTimeFrameReadoutBuilder lStfBuilder(lOutputChan, mDevice.dplEnabled());
  lStfBuilder.setRdh4FilterTrigger(mRdh4FilterTrigger);

  using hres_clock = std::chrono::high_resolution_clock;
  auto lStfStartTime = hres_clock::now();

  try {
    while (mRunning) {

      // Equipment ID for the HBFrames (from the header)
      ReadoutSubTimeframeHeader lReadoutHdr;
      lReadoutMsgs.clear();

      // receive readout messages
      const auto lRet = lInputChan.Receive(lReadoutMsgs);
      if (lRet < 0 && mRunning) {
        // LOG(WARNING) << "StfHeader receive failed (err = " + std::to_string(lRet) + ")";
        std::this_thread::yield();
        continue;
      } else if (lRet < 0) {
        break; // should exit?
      }

      // Copy to avoid surprises. The receiving header is not O2 compatible and can be discarded
      assert(lReadoutMsgs[0]->GetSize() == sizeof(ReadoutSubTimeframeHeader));
      std::memcpy(&lReadoutHdr, lReadoutMsgs[0]->GetData(), sizeof(ReadoutSubTimeframeHeader));

      // check multipart size
      {
        if (lReadoutHdr.mNumberHbf != (lReadoutMsgs.size() - 1)) {
          LOG(ERROR) << "READOUT INTERFACE: indicated number of HBFrames in the header does not match "
                        " the number of sent blocks: "
                     << lReadoutHdr.mNumberHbf << " != " << (lReadoutMsgs.size() - 1);
        }

        if (lReadoutMsgs.size() > 1) {
          [[maybe_unused]]
          const auto [lCruId, lEndPoint, lLinkId] = ReadoutDataUtils::getSubSpecificationComponents(
            static_cast<const char*>(lReadoutMsgs[1]->GetData()),
            lReadoutMsgs[1]->GetSize());

          if (lLinkId != lReadoutHdr.mLinkId) {
            LOG(ERROR) << "READOUT INTERFACE: indicated link ID does not match RDH in data block "
                       << (unsigned)lReadoutHdr.mLinkId << " != " << lLinkId;
          }
        }
      }

      if (lReadoutMsgs.size() < 2) {
        LOG(WARNING) << "READOUT INTERFACE: no data sent.";
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

        { // MON: data of a new STF received, get the freq and new start time
          if (mDevice.guiEnabled()) {
            const auto lStfDur = std::chrono::duration<float>(hres_clock::now() - lStfStartTime);
            mStfFreqSamples.Fill(1.0f / lStfDur.count());
            lStfStartTime = hres_clock::now();
          }
        }

        if (lCurrentStfId > 0 && lReadoutHdr.mTimeFrameId < lCurrentStfId) {
          LOG(ERROR) << "TF ID decreased! (" << lCurrentStfId << ") -> (" << lReadoutHdr.mTimeFrameId << ") "
                        " readout.exe sent messages with non-monotonic TF id!. SubTimeFrames will be incomplete! "
                        "Please report the BUG.";
        }

        if (lCurrentStfId >= 0) {
          // Finished: queue the current STF and start a new one
          std::unique_ptr<SubTimeFrame> lStf = lStfBuilder.getStf();
          if (lStf) {
            // LOG(DEBUG) << "Received TF[" << lStf->header().mId<< "]::size= " << lStf->getDataSize();
            mDevice.queue(eStfBuilderOut, std::move(lStf));
          } else {
            LOG(INFO) << "No data received? This should not happen. ";
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
  } catch (std::runtime_error& e) {
    LOG(ERROR) << "Receive failed. Stopping input thread[" << pInputChannelIdx << "]...";
    return;
  }

  LOG(INFO) << "Exiting input thread[" << pInputChannelIdx << "]...";
}

}
}
