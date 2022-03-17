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
#include <ConfigConsul.h>

#include <DataDistLogger.h>
#include <DataDistributionOptions.h>

#include <FairMQDevice.h>

#include <vector>
#include <queue>
#include <chrono>
#include <sstream>

namespace o2::DataDistribution
{

void StfInputInterface::start(bool pBuildStf, std::shared_ptr<ConsulStfBuilder> pStfBuilderConfig)
{
  mRunning = true;
  mBuildStf = pBuildStf;
  mDiscoveryConfig = pStfBuilderConfig;

  mBuilderInputQueue = std::make_unique<ConcurrentFifo<std::vector<FairMQMessagePtr>>>();
  mStfBuilder = std::make_unique<SubTimeFrameReadoutBuilder>(mDevice.MemI());

  // sequence thread only needed when building physics STFs
  if (pBuildStf) {
    mSeqStfQueue.start();
    mStfSeqThread = create_thread_member("stfb_seq", &StfInputInterface::StfSequencerThread, this);
  }

  if (pBuildStf) {
    mBuilderThread = create_thread_member("stfb_builder", &StfInputInterface::StfBuilderThread, this);
  } else  {
    mBuilderThread = create_thread_member("equip_builder", &StfInputInterface::TopologicalStfBuilderThread, this);
  }

  mInputThread = create_thread_member("stfb_input", &StfInputInterface::StfReceiverThread, this);
}

void StfInputInterface::stop()
{
mRunning = false;

  mStfBuilder->stop();


  if (mInputThread.joinable()) {
    mInputThread.join();
  }

  mBuilderInputQueue->stop();

  if (mBuilderThread.joinable()) {
      mBuilderThread.join();
  }

  if (mStfSeqThread.joinable()) {
    mSeqStfQueue.stop();
    mStfSeqThread.join();
  }

  mBuilderInputQueue.reset();
  mStfBuilder.reset();

  DDDLOG("INPUT INTERFACE: Stopped.");
}

/// Receiving thread
void StfInputInterface::StfReceiverThread()
{
  using namespace std::chrono_literals;
  std::vector<FairMQMessagePtr> lReadoutMsgs;
  lReadoutMsgs.reserve(4096);

  // Reference to the input channel
  auto& lInputChan = mDevice.GetChannel(mDevice.getInputChannelName());

  try {
    while (mRunning) {

      // Equipment ID for the HBFrames (from the header)
      ReadoutSubTimeframeHeader lReadoutHdr;
      lReadoutMsgs.clear();

      // receive readout messages
      const std::int64_t lRet = lInputChan.Receive(lReadoutMsgs, 100);

      // timeout ok
      if (lRet == static_cast<int64_t>(fair::mq::TransferCode::timeout)) {
        continue;
      }
      // interrupted
      if (lRet == static_cast<int64_t>(fair::mq::TransferCode::interrupted)) {
        if (mAcceptingData) {
          IDDLOG_RL(1000, "READOUT INTERFACE: Receive failed. FMQ state interrupted.");
        }
        std::this_thread::sleep_for(100ms);
        continue;
      }

      // not in running state
      if (lRet > 0 && !mAcceptingData) {
        WDDLOG_RL(1000, "READOUT INTERFACE: Discarding received data because not in the FMQ:RUNNING state.");
        continue;
      }

      // error
      if (lRet == static_cast<int64_t>(fair::mq::TransferCode::error)) {
        EDDLOG_RL(1000, "READOUT INTERFACE: Receive failed. fmq_error={} errno={} error={}",
          lRet, errno, std::string(strerror(errno)));
        std::this_thread::sleep_for(10ms);
        continue;
      }

      assert (lRet >= 0 && mAcceptingData);

      if (lReadoutMsgs.empty()) {
        // nothing received?
        continue;
      }

      // Copy to avoid surprises. The receiving header is not O2 compatible and can be discarded
      if (lReadoutMsgs[0]->GetSize() != sizeof(ReadoutSubTimeframeHeader)) {
        EDDLOG_RL(1000, "READOUT INTERFACE: incompatible readout header received. "
          "Make sure to use compatible o2-readout-exe version. received_size={} expected_size={}",
          lReadoutMsgs[0]->GetSize(), sizeof(ReadoutSubTimeframeHeader));
        continue;
      }
      std::memcpy(&lReadoutHdr, lReadoutMsgs[0]->GetData(), sizeof(ReadoutSubTimeframeHeader));

      // check the readout header version
      if (lReadoutHdr.mVersion != sReadoutInterfaceVersion) {
        EDDLOG_RL(1000, "READOUT INTERFACE: Unsupported readout interface version. "
          "Make sure to use compatible o2-readout-exe version. received={} expected={}",
          lReadoutHdr.mVersion, sReadoutInterfaceVersion);
        continue;
      }

      // check for backward/forward tf jumps
      if (mBuildStf) {
        // backward jump
        if (lReadoutHdr.mTimeFrameId < mStfIdReceiving) {
          EDDLOG_RL(1000, "READOUT INTERFACE: STF ID decreased, data cannot be aggregated! {} -> {}",
            mStfIdReceiving, lReadoutHdr.mTimeFrameId);
          continue;
        }

        // forward jump
        if (lReadoutHdr.mTimeFrameId > (mStfIdReceiving + 1)) {
          WDDLOG_RL(1000, "READOUT INTERFACE: Non-contiguous increase of STF ID! {} -> {}",
            mStfIdReceiving, lReadoutHdr.mTimeFrameId);
          // we keep the data since this might be a legitimate jump
        }

        // get the current TF id
        mStfIdReceiving = lReadoutHdr.mTimeFrameId;
      }

      mBuilderInputQueue->push(std::move(lReadoutMsgs));
    }
  } catch (std::runtime_error& e) {
    if (mRunning) {
      EDDLOG_RL(1000, "Receive failed on the Input channel. Stopping the input thread. what={}", e.what());
    }
  }

  DDDLOG("Exiting the input thread.");
}

/// StfBuilding thread
void StfInputInterface::StfBuilderThread()
{
  using namespace std::chrono_literals;

  bool lStarted = false;
  std::vector<FairMQMessagePtr> lReadoutMsgs;
  lReadoutMsgs.reserve(1U << 20);

  // support FEEID masking
  std::uint32_t lFeeIdMask = ~std::uint32_t(0); // subspec size
  const auto lFeeMask = std::getenv("DATADIST_FEE_MASK");
  if (lFeeMask) {
    try {
      lFeeIdMask = std::stoul(lFeeMask, nullptr, 16);
    } catch(...) {
      EDDLOG("Cannot convert {} for the FeeID mask.", lFeeMask);
    }
  }
  IDDLOG("StfBuilder: Using {:#06x} as the FeeID mask.", lFeeIdMask);

  // Reference to the input channel
  assert (mBuilderInputQueue);
  assert (mStfBuilder);
  // Input queue
  auto &lInputQueue = *mBuilderInputQueue;
  // Stf builder
  SubTimeFrameReadoutBuilder &lStfBuilder = *mStfBuilder;

  // insert and mask the feeid
  auto lInsertWithFeeIdMasking = [&lStfBuilder, lFeeIdMask] (const header::DataOrigin &pDataOrigin,
    const header::DataHeader::SubSpecificationType &pSubSpec, const ReadoutSubTimeframeHeader &pRdoHeader,
    const FairMQParts::iterator pStartHbf, const std::size_t pInsertCnt) {

    // mask the subspecification if the fee mode is used
    auto lMaskedSubspec = pSubSpec;
    if (ReadoutDataUtils::SubSpecMode::eFeeId == ReadoutDataUtils::sRawDataSubspectype) {
      lMaskedSubspec &= lFeeIdMask;
    }

    lStfBuilder.addHbFrames(pDataOrigin, lMaskedSubspec, pRdoHeader, pStartHbf, pInsertCnt);

    return pInsertCnt;
  };

  const auto cStfDataWaitFor = 2s;
  DDMON_RATE("stfbuilder", "stf_input", 0.0);

  while (mRunning) {

    // Lambda for completing the Stf
    auto finishBuildingCurrentStf = [&](bool pTimeout = false) {
      // Finished: queue the current STF and start a new one
      ReadoutDataUtils::sFirstSeenHBOrbitCnt = 0;

      if (auto lStf = lStfBuilder.getStf(); lStf.has_value()) {
        // start the new STF
        if (pTimeout) {
          WDDLOG("READOUT INTERFACE: finishing STF on a timeout. stf_id={} size={}",
            (*lStf)->header().mId, (*lStf)->getDataSize());
        }

        (*lStf)->setOrigin(SubTimeFrame::Header::Origin::eReadout);

        DDMON_RATE("stfbuilder", "stf_input", (*lStf)->getDataSize());
        mSeqStfQueue.push(std::move(*lStf));
      }
    };

    // Equipment ID for the HBFrames (from the header)
    lReadoutMsgs.clear();

    // receive readout messages
    const auto lRet = lInputQueue.pop_wait_for(lReadoutMsgs, cStfDataWaitFor);
    if (!lRet && mRunning) {
      if (lStarted) {
        // finish on a timeout
        finishBuildingCurrentStf(true /* timeout */);
      }
      continue;
    } else if (!lRet && !mRunning) {
      break;
    } else if (lRet && !mRunning) {
      static thread_local std::uint64_t sAfterStopStfs = 0;
      sAfterStopStfs++;
      WDDLOG_RL(1000, "StfBuilderThread: Building STFs after stop signal. after_stop_stf_count={}", sAfterStopStfs);
    }

    // must not be empty
    if (lReadoutMsgs.empty()) {
      EDDLOG_RL(1000, "READOUT INTERFACE: empty readout multipart.");
      continue;
    }

    // stated to build STFs
    lStarted = true;

    // Copy to avoid surprises. The receiving header is not O2 compatible and can be discarded
    ReadoutSubTimeframeHeader lReadoutHdr;
    // NOTE: the size is checked on receive
    std::memcpy(&lReadoutHdr, lReadoutMsgs[0]->GetData(), sizeof(ReadoutSubTimeframeHeader));

    // log only
    DDDLOG_RL(5000, "READOUT INTERFACE: Received an ReadoutMsg. stf_id={}", lReadoutHdr.mTimeFrameId);

    // check multipart size
    if (lReadoutMsgs.size() == 1 && !lReadoutHdr.mFlags.mLastTFMessage) {
      EDDLOG_RL(1000, "READOUT INTERFACE: Received only a header message without the STF stop bit set.");
      continue;
    }

    // check the link/feeids (first HBF only)
    if (lReadoutMsgs.size() > 1 && lReadoutHdr.mFlags.mIsRdhFormat) {
      try {
        const auto R = RDHReader(lReadoutMsgs[1]);
        const auto lLinkId = R.getLinkID();

        if (lLinkId != lReadoutHdr.mLinkId) {
          EDDLOG_RL(1000, "READOUT INTERFACE: Update link ID does not match RDH in the data block."
            " hdr_link_id={} rdh_link_id={}", lReadoutHdr.mLinkId, lLinkId);
        }
      } catch (RDHReaderException &e) {
        EDDLOG_RL(1000, "READOUT INTERFACE: error while parsing the RDH header. what={}", e.what());
        // TODO: the whole ReadoutMsg is discarded. Account and report the data size.
        continue;
      }
    }

    const auto lIdInBuilding = lStfBuilder.getCurrentStfId();
    mStfIdBuilding = lIdInBuilding ? *lIdInBuilding : lReadoutHdr.mTimeFrameId;

    // check for the new TF marker
    if (lReadoutHdr.mTimeFrameId != mStfIdBuilding) {
      // we expect to be notified about new TFs
      if (lIdInBuilding) {
        EDDLOG_RL(1000, "READOUT INTERFACE: Update with a new STF ID but the Stop flag was not set for the current STF."
          " current_id={} new_id={}", mStfIdBuilding, lReadoutHdr.mTimeFrameId);
        finishBuildingCurrentStf();
      }
      mStfIdBuilding = lReadoutHdr.mTimeFrameId;
    }

    const bool lFinishStf = lReadoutHdr.mFlags.mLastTFMessage;
    if (lReadoutMsgs.size() > 1) {
      // check subspecifications of all messages
      header::DataHeader::SubSpecificationType lSubSpecification = ~header::DataHeader::SubSpecificationType(0);
      header::DataOrigin lDataOrigin;
      try {
        const auto R1 = RDHReader(lReadoutMsgs[1]);
        lDataOrigin = ReadoutDataUtils::getDataOrigin(R1);
        lSubSpecification = ReadoutDataUtils::getSubSpecification(R1);
      } catch (RDHReaderException &e) {
        EDDLOG_RL(1000, "READOUT INTERFACE: Cannot parse RDH of received HBFs. what={}", e.what());
        // TODO: the whole ReadoutMsg is discarded. Account and report the data size.
        continue;
      }

      assert (lReadoutMsgs.size() > 1);
      auto lStartHbf = lReadoutMsgs.begin() + 1; // skip the meta message
      auto lEndHbf = lStartHbf + 1;

      std::size_t lAdded = 0;
      bool lErrorWhileAdding = false;

      while (true) {
        if (lEndHbf == lReadoutMsgs.end()) {
          //insert the remaining span
          std::size_t lInsertCnt = (lEndHbf - lStartHbf);
          lAdded += lInsertWithFeeIdMasking(lDataOrigin, lSubSpecification, lReadoutHdr, lStartHbf, lInsertCnt);
          break;
        }

        header::DataHeader::SubSpecificationType lNewSubSpec = ~header::DataHeader::SubSpecificationType(0);
        try {
          const auto Rend = RDHReader(*lEndHbf);
          lNewSubSpec = ReadoutDataUtils::getSubSpecification(Rend);
        } catch (RDHReaderException &e) {
          EDDLOG_RL(1000, e.what());
          // TODO: portion of the ReadoutMsg is discarded. Account and report the data size.
          lErrorWhileAdding = true;
          break;
        }

        if (lNewSubSpec != lSubSpecification) {
          WDDLOG_RL(10000, "READOUT INTERFACE: Update with mismatched subspecifications."
            " block[0]_subspec={:#06x}, block[{}]_subspec={:#06x}",
            lSubSpecification, (lEndHbf - (lReadoutMsgs.begin() + 1)), lNewSubSpec);
          // insert
          lAdded += lInsertWithFeeIdMasking(lDataOrigin, lSubSpecification, lReadoutHdr, lStartHbf, lEndHbf - lStartHbf);
          lStartHbf = lEndHbf;

          lSubSpecification = lNewSubSpec;
        }
        lEndHbf = lEndHbf + 1;
      }

      if (!lErrorWhileAdding && (lAdded != lReadoutMsgs.size() - 1) ) {
        EDDLOG_RL(500, "BUG: Not all received HBFrames added to the STF.");
      }
    }

    // check if this was the last message of an STF
    if (lFinishStf) {
      finishBuildingCurrentStf();
    }
  }

  DDDLOG("Exiting StfBuilder thread.");
}


/// StfBuilding thread for ITS/MFT threshold scan
void StfInputInterface::TopologicalStfBuilderThread()
{
  using namespace std::chrono_literals;

  bool lStarted = false;
  std::vector<FairMQMessagePtr> lReadoutMsgs(1U << 20);
  lReadoutMsgs.clear();

  // per equipment data cache
  std::map<EquipmentIdentifier, std::unique_ptr<SubTimeFrame> > lEquipmentStfs;

  // support FEEID masking
  std::uint32_t lFeeIdMask = ~std::uint32_t(0); // subspec size
  const auto lFeeMask = std::getenv("DATADIST_FEE_MASK");
  if (lFeeMask) {
    try {
      lFeeIdMask = std::stoul(lFeeMask, nullptr, 16);
    } catch(...) {
      EDDLOG("Cannot convert {} for the FeeID mask.", lFeeMask);
    }
  }
  IDDLOG("StfBuilder: Using {:#06x} as the FeeID mask.", lFeeIdMask);

  // Reference to the input channel
  assert (mBuilderInputQueue);
  assert (mStfBuilder);
  // Input queue
  auto &lInputQueue = *mBuilderInputQueue;
  // Stf builder
  SubTimeFrameReadoutBuilder &lStfBuilder = *mStfBuilder;

  // get number of pages for per link aggregation
  const uint64_t lNumPagesInThresholdScanStf = std::clamp(
    mDiscoveryConfig->getUInt64Param(NumPagesInTopologicalStfKey, NumPagesInTopologicalStfDefault),
    std::uint64_t(1),
    std::uint64_t(50000)
  );

  // get a flag for cutting topo TFs on a new orbit
  const bool lCutOnNewOrbit = mDiscoveryConfig->getBoolParam(StartTopologicalStfOnNewOrbitfKey, StartTopologicalStfOnNewOrbitDefault);

  IDDLOG("Aggregating pages for each link in topological STF building. num_pages={} only_new_orbit={}",
    lNumPagesInThresholdScanStf, lCutOnNewOrbit);

  // insert and mask the feeid. Return the Stf if aggregation is reached
  // Note call again if lCutOnNewOrbit is not 0
  auto lInsertTopoWithFeeIdMasking = [&lStfBuilder, lFeeIdMask, lNumPagesInThresholdScanStf, lCutOnNewOrbit] (
    const header::DataOrigin &pDataOrigin,
    const header::DataHeader::SubSpecificationType &pSubSpec,
    const ReadoutSubTimeframeHeader &pRdoHeader,
    FairMQParts::iterator &pStartHbf /* in/out */,
    std::size_t &pInsertCnt /* in/out */) -> std::optional<std::unique_ptr<SubTimeFrame> > {

    // mask the subspecification if the fee mode is used
    auto lMaskedSubspec = pSubSpec;
    if (ReadoutDataUtils::SubSpecMode::eFeeId == ReadoutDataUtils::sRawDataSubspectype) {
      lMaskedSubspec &= lFeeIdMask;
    }

    return lStfBuilder.addTopoStfData(pDataOrigin, lMaskedSubspec, pRdoHeader, pStartHbf, pInsertCnt,
      lNumPagesInThresholdScanStf, lCutOnNewOrbit);
  };

  const auto cStfDataWaitFor = 500ms;
  DDMON_RATE("stfbuilder", "stf_input", 0.0);

  auto queueStf = [&] (std::unique_ptr<SubTimeFrame> pStf) {
    // static auto lStartSec = clock::now();

    static std::uint64_t sStfId = 0;
    if (pStf) {
      // make sure we queue STFs in ascending order
      sStfId += 1;
      pStf->updateId(sStfId);
      pStf->setOrigin(SubTimeFrame::Header::Origin::eReadoutTopology);

      DDMON_RATE("stfbuilder", "stf_input", pStf->getDataSize());
      mDevice.I().queue(eStfBuilderOut, std::move(pStf));
    }
  };

  // Lambda for completing the Stf
  auto finishBuildingCurrentStfs = [&]() {
    // Finished: queue the current STF and start a new one
    while (true) {
      auto lStfOpt = lStfBuilder.getTopoStf();
      if (!lStfOpt) {
        break;
      }
      // queue if not null
      queueStf(std::move(*lStfOpt));
    }
  };

  while (mRunning) {
    // Equipment ID for the HBFrames (from the header)
    lReadoutMsgs.clear();

    // receive readout messages
    const auto lRet = lInputQueue.pop_wait_for(lReadoutMsgs, cStfDataWaitFor);
    if (!lRet && mRunning) {
      if (lStarted) {
        // finish on a timeout
        finishBuildingCurrentStfs();
      }

      continue;
    } else if (!lRet && !mRunning) {
      break;
    } else if (lRet && !mRunning) {
      static thread_local std::uint64_t sAfterStopStfs = 0;
      sAfterStopStfs++;
      WDDLOG_RL(1000, "StfBuilderThread: Building STFs after stop signal. after_stop_stf_count={}", sAfterStopStfs);
    }

    // must not be empty
    if (lReadoutMsgs.size() <= 1) {
      EDDLOG_RL(1000, "READOUT INTERFACE: empty readout multipart.");
      continue;
    }

    // stated to build STFs
    lStarted = true;

    // Copy to avoid surprises. The receiving header is not O2 compatible and can be discarded
    ReadoutSubTimeframeHeader lReadoutHdr;
    // NOTE: the size is checked on receive
    std::memcpy(&lReadoutHdr, lReadoutMsgs[0]->GetData(), sizeof(ReadoutSubTimeframeHeader));

    // log only
    DDDLOG_RL(5000, "READOUT INTERFACE: Received an ReadoutMsg. stf_id={}", lReadoutHdr.mTimeFrameId);

    // check multipart size
    if (lReadoutMsgs.size() == 1 && !lReadoutHdr.mFlags.mLastTFMessage) {
      EDDLOG_RL(1000, "READOUT INTERFACE: Received only a header message without the STF stop bit set.");
      continue;
    }

    // get the subspecification
    header::DataHeader::SubSpecificationType lSubSpecification = ~header::DataHeader::SubSpecificationType(0);
    header::DataOrigin lDataOrigin;
    try {
      const auto R1 = RDHReader(lReadoutMsgs[1]);
      lDataOrigin = ReadoutDataUtils::getDataOrigin(R1);
      lSubSpecification = ReadoutDataUtils::getSubSpecification(R1);
    } catch (RDHReaderException &e) {
      EDDLOG_RL(1000, "READOUT INTERFACE: Cannot parse RDH of received HBFs. what={}", e.what());
      // TODO: the whole ReadoutMsg is discarded. Account and report the data size.
      continue;
    }

    auto lStartHbf = lReadoutMsgs.begin() + 1; // skip the meta message
    std::size_t lInsertCnt = lReadoutMsgs.size() - 1;

    // insert all HBFs
    while (lInsertCnt > 0) {
      auto lStfOpt = lInsertTopoWithFeeIdMasking(lDataOrigin, lSubSpecification, lReadoutHdr, lStartHbf, lInsertCnt);
      if (lStfOpt) {
        queueStf(std::move(*lStfOpt));
      } else {
        assume(lInsertCnt == 0);
      }
    }
  }

  DDDLOG("Exiting EquipmentBuilderThread thread.");
}


void StfInputInterface::StfSequencerThread()
{
  using namespace std::chrono_literals;

  static constexpr std::uint64_t sMaxMissingStfsForSeq = 2ull * 11234 / 256; // 2 seconds of STFs

  std::uint64_t lMissingStfs = 0;

  while (mRunning) {
    auto lStf = mSeqStfQueue.pop_wait_for(500ms);

    // monitoring cumulative metric
    DDMON("stfbuilder", "stf_input.missing.total", lMissingStfs);

    if (lStf == std::nullopt || !mAcceptingData) {
      continue;
    }

    // have data, check the sequence
    if (lStf) {
      const auto lCurrId = (*lStf)->id();

      if (lCurrId <= mLastSeqStfId) {
        EDDLOG_RL(500, "READOUT INTERFACE: Repeated STF will be rejected. previous_stf_id={} current_stf_id={}",
          mLastSeqStfId, lCurrId);
        // reject this STF.
        continue;
      }

      // expected next stf
      if ((mLastSeqStfId + 1) == lCurrId) {
        mLastSeqStfId = lCurrId;
        mDevice.I().queue(eStfBuilderOut, std::move(*lStf));
        continue;
      }

      // there are missing STFs
      const auto lMissingIdStart = mLastSeqStfId + 1;
      const auto lMissingCnt = lCurrId - lMissingIdStart;

      if (lMissingCnt < sMaxMissingStfsForSeq) {
        WDDLOG_RL(1000, "READOUT INTERFACE: Creating empty (missing) STFs. previous_stf_id={} num_missing={}",
          mLastSeqStfId, lMissingCnt);
        // create the missing ones and continue
        for (std::uint64_t lStfIdIdx = lMissingIdStart; lStfIdIdx < lCurrId; lStfIdIdx++) {
          auto lEmptyStf = std::make_unique<SubTimeFrame>(lStfIdIdx);
          lEmptyStf->setOrigin(SubTimeFrame::Header::Origin::eNull);
          mDevice.I().queue(eStfBuilderOut, std::move(lEmptyStf));

          lMissingStfs++;
        }
      } else {
        WDDLOG_RL(1000, "READOUT INTERFACE: Large STF gap. previous_stf_id={} current_stf_id={} num_missing={}",
          mLastSeqStfId, lCurrId, lMissingCnt);
      }

      // insert the actual stf
      mLastSeqStfId = lCurrId;
      mDevice.I().queue(eStfBuilderOut, std::move(*lStf));

      continue;
    }
  }

  DDDLOG("Exiting StfSequencerThread thread.");
}

} /* namespace o2::DataDistribution */
