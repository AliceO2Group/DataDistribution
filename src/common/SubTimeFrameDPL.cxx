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

#include "SubTimeFrameDPL.h"

#include "DataDistLogger.h"

#include <Framework/DataProcessingHeader.h>
#include <Framework/SourceInfoHeader.h>
#include <Headers/Stack.h>

namespace o2::DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// StfDplAdapter
////////////////////////////////////////////////////////////////////////////////

void StfToDplAdapter::visit(SubTimeFrame& pStf, void*)
{
  // Pack the Stf header
  o2::header::DataHeader lStfDistDataHeader(
    gDataDescSubTimeFrame,
    o2::header::gDataOriginFLP,
    0, // Subspecification: not used by consumers
    sizeof(SubTimeFrame::Header)
  );

  lStfDistDataHeader.payloadSerializationMethod = gSerializationMethodNone;
  lStfDistDataHeader.tfCounter = pStf.header().mId;
  lStfDistDataHeader.runNumber = pStf.header().mRunNumber;
  lStfDistDataHeader.firstTForbit = pStf.header().mFirstOrbit;

  DDDLOG_RL(10000, "DPL sending a TimeFrame. tf_id={} run_number={} first_orbit={}",
    pStf.header().mId, pStf.header().mRunNumber, pStf.header().mFirstOrbit);

  o2::framework::DataProcessingHeader lDplHeader(pStf.header().mId);
  lDplHeader.creation = pStf.header().mCreationTimeMs;

  { // prepare DistDataHeader
    auto lHdrStack = Stack(lStfDistDataHeader, lDplHeader);

    auto lDataHeaderMsg = mMemRes.newHeaderMessage(lHdrStack.data(), lHdrStack.size());
    if (!lDataHeaderMsg) {
      EDDLOG("Allocation error: Stf DataHeader. size={}", sizeof(DataHeader));
      throw std::bad_alloc();
    }

    auto lDataMsg = mMemRes.newHeaderMessage(&pStf.header(), sizeof(SubTimeFrame::Header));
    if (!lDataMsg) {
      EDDLOG("Allocation error: Stf::Header. size={}", sizeof(SubTimeFrame::Header));
      throw std::bad_alloc();
    }

    mMessages.push_back(std::move(lDataHeaderMsg));
    mMessages.push_back(std::move(lDataMsg));
  }

  // Send data in lexicographical order of DataIdentifier + subSpecification
  // for easier binary comparison
  std::vector<EquipmentIdentifier> lEquipIds = pStf.getEquipmentIdentifiers();
  std::sort(std::begin(lEquipIds), std::end(lEquipIds));

  for (const auto& lEquip : lEquipIds) {

    auto& lHBFrameVector = pStf.mData.at(lEquip).at(lEquip.mSubSpecification);

    if (lHBFrameVector.empty()) {
      continue;
    }

    // all messages are already in the correct format
    for (std::size_t i = 0; i < lHBFrameVector.size(); i++) {

      if (mReducedHdr) {
        mMessages.push_back(std::move(lHBFrameVector[i].mHeader));

        std::move(std::begin(lHBFrameVector[i].mDataParts), std::end(lHBFrameVector[i].mDataParts),
          std::back_inserter(mMessages));
      } else {
        auto &lMssg = lHBFrameVector[i];

        // single messages
        if (lMssg.mDataParts.size() == 1) {
          auto lDhPtr = lMssg.getDataHeaderMutable();
          lDhPtr->splitPayloadIndex = 0;
          lDhPtr->splitPayloadParts = 1;
          lDhPtr->payloadSize = lMssg.mDataParts.front()->GetSize();

          mMessages.push_back(std::move(lMssg.mHeader));
          mMessages.push_back(std::move(lMssg.mDataParts.front()));
        } else {
          // non-reduced headers
          auto lDhPtr = lMssg.getDataHeaderMutable();

          // take all but last data message to reuse the existing hdr message
          for (std::size_t iSp = 0; iSp < lMssg.mDataParts.size() - 1; iSp += 1) {
            auto &lDataMsg = lMssg.mDataParts[iSp];
            lDhPtr->splitPayloadIndex = iSp;
            lDhPtr->splitPayloadParts = lMssg.mDataParts.size();
            lDhPtr->payloadSize = lDataMsg->GetSize();

            auto lSpHdr = mMemRes.newHeaderMessage(lMssg.mHeader->GetData(), lMssg.mHeader->GetSize());
            if (!lSpHdr) {
              throw std::bad_alloc();
            }
            mMessages.push_back(std::move(lSpHdr));
            mMessages.push_back(std::move(lDataMsg));
          }
          // add the last message
          lDhPtr->splitPayloadIndex = lMssg.mDataParts.size() - 1;
          lDhPtr->splitPayloadParts = lMssg.mDataParts.size();
          lDhPtr->payloadSize = lMssg.mDataParts.back()->GetSize();

          mMessages.push_back(std::move(lMssg.mHeader));
          mMessages.push_back(std::move(lMssg.mDataParts.back()));
        }
      }
    }

    lHBFrameVector.clear();
  }

  pStf.clear();
}

void StfToDplAdapter::inspect() const
{
  // check validity of the TF message sequence

  DataHeader::TForbitType lFirstTForbit = ~DataHeader::TForbitType{0};
  DataHeader::TFCounterType lTfCounter = ~DataHeader::TFCounterType{0};
  DataHeader::RunNumberType lRunNumber = ~DataHeader::RunNumberType{0};

  o2::framework::DataProcessingHeader::StartTime lProcStart = ~ o2::framework::DataProcessingHeader::StartTime{0};

  size_t lIdx = 0;
  while (lIdx < mMessages.size()) {

    // Check the O2 DataHeader
    const DataHeader *lDh = o2::header::get<DataHeader*>(mMessages[lIdx]->GetData(), mMessages[lIdx]->GetSize());
    if (!lDh) {
      EDDLOG("DPL output: wrong message instead of DataHeader. index={} size={}", lIdx, mMessages[lIdx]->GetSize());
      lIdx++;
      continue;
    }

    if (lTfCounter == ~DataHeader::TFCounterType{0}) {
      lFirstTForbit = lDh->firstTForbit;
      lTfCounter = lDh->tfCounter;
      lRunNumber = lDh->runNumber;
    } if (lFirstTForbit == 0) { // not all things have the first orbit
      lFirstTForbit = lDh->firstTForbit;
    }

    if ((lDh->firstTForbit != 0) && lFirstTForbit != lDh->firstTForbit) {
      EDDLOG("DPL output: first orbit is different first={} != current={}", lFirstTForbit, lDh->firstTForbit);
    }

    if (lTfCounter != lDh->tfCounter) {
      EDDLOG("DPL output: TF counter is different first={} != current={}", lTfCounter, lDh->tfCounter);
    }

    if (lRunNumber != lDh->runNumber) {
      EDDLOG("DPL output: run number is different first={} != current={}", lRunNumber, lDh->runNumber);
    }

    // Check the DPL header
    const auto *lProcHdr = o2::header::get<o2::framework::DataProcessingHeader*>(mMessages[lIdx]->GetData(), mMessages[lIdx]->GetSize());
    if (!lProcHdr){
      EDDLOG("DPL output: Mising processing header (DPL). index={} size={}", lIdx, mMessages[lIdx]->GetSize());
      lIdx += 2;
      continue;
    }

    if (lProcStart == ~ o2::framework::DataProcessingHeader::StartTime{0}) {
      lProcStart = lProcHdr->startTime;
    } else {
      if (lProcStart != lProcHdr->startTime) {
        EDDLOG("DPL output: dpl proc header start is different first={} != current={}", lProcStart, lProcHdr->startTime);
      }
    }

    if (lDh->splitPayloadParts == 0) {
      WDDLOG("DPLcheck: splitPayloadParts != 1");
    }

    if (lDh->splitPayloadParts == 1) { // skip single message + data
      lIdx+=2;
      continue;
    }

    // check we have enough messsages
    if (lDh->splitPayloadParts > (mMessages.size() - lIdx) ) {
      EDDLOG("DPL output: the multi-part is too short for the split-payload message. "
        "splitpayload_size={} tf_available={} tf_total={}", lDh->splitPayloadParts, (mMessages.size() - lIdx), mMessages.size());
      break;
    }

    // check the first message
    if (! ((lDh->splitPayloadIndex == 0) || (lDh->splitPayloadIndex == lDh->splitPayloadParts)) ) {
      EDDLOG("DPL output: index of the first split-payload message is invalid. index={} parts={}",
        lDh->splitPayloadIndex, lDh->splitPayloadParts);
      break;
    }

    if (lDh->splitPayloadParts > 1 && (lDh->splitPayloadIndex == lDh->splitPayloadParts)) {
      // skip split part messages + hdr
      lIdx += (lDh->splitPayloadParts + 1);
    } else if (lDh->splitPayloadParts > 1 && (lDh->splitPayloadIndex == 0)) {
      // non-reduced split he
      // skip split part messages + hdr
      lIdx += (lDh->splitPayloadParts * 2);
    } else {
      // single message
      lIdx += 2;
    }
  }

  // DEBUG: TFs with first tf orbit == 0
  if (lFirstTForbit == 0) {
    WDDLOG_RL(500, "DPL output: TF with FirstTForbit == 0. run={} dh_tf_counter={} num_messages={}", lRunNumber, lTfCounter, mMessages.size());
  }
}

void StfToDplAdapter::sendToDpl(std::unique_ptr<SubTimeFrame>&& pStf)
{
  if (!mRunning) {
    return;
  }

  mMessages.clear();
  pStf->accept(*this);

#if 0
  DDDLOG("Content of the Stf:");
  uint64_t lMsgIdx = 0;
  for (auto lM = mMessages.cbegin(); lM != mMessages.cend(); ) {
    DDDLOG("  o2: message #{}", lMsgIdx++);
    o2::header::hexDump("o2 header", (*lM)->GetData(), (*lM)->GetSize());
    lM++;
    o2::header::hexDump("o2 payload", (*lM)->GetData(), std::clamp((*lM)->GetSize(), std::size_t(0), std::size_t(256)) );
    lM++;
  }
#endif

  if (mInspectChannel) {
    inspect();
  }

  mChan.Send(mMessages);

  // make sure headers and chunk pointers don't linger
  mMessages.clear();
  pStf.reset();
}


void StfToDplAdapter::sendEosToDpl()
{
  o2::framework::SourceInfoHeader lDplExitHdr;
  lDplExitHdr.state = o2::framework::InputChannelState::Completed;

  o2::framework::DataProcessingHeader lDplHdr;
  lDplHdr.creation = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();

  const auto lDoneStack = o2::header::Stack(
    o2::header::DataHeader(o2::header::gDataDescriptionInfo, o2::header::gDataOriginAny, 0, 0),
    lDplHdr,
    lDplExitHdr
  );

  IDDLOG("Sending End of stream (EoS) message to {} receivers.", mChan.GetNumberOfConnectedPeers());

  // Send a multiparts
  for (auto lEosCnt = 0U; lEosCnt < mChan.GetNumberOfConnectedPeers(); lEosCnt++) {
    FairMQParts lCompletedMsg;
    auto lNoFree = [](void*, void*) { /* stack */ };
    lCompletedMsg.AddPart(mChan.NewMessage(lDoneStack.data(), lDoneStack.size(), lNoFree));
    lCompletedMsg.AddPart(mChan.NewMessage());
    auto lRet = mChan.Send(lCompletedMsg, 2000);

    IDDLOG("End of stream (EoS) message sent to DPL. eos_id={} send_ret={}", lEosCnt, lRet);
  }
}

////////////////////////////////////////////////////////////////////////////////
/// DplToStfAdapter
////////////////////////////////////////////////////////////////////////////////

static const o2::header::DataHeader gStfDistDataHeader(
  gDataDescSubTimeFrame,
  o2::header::gDataOriginFLP,
  0, // subspecification: not used
  sizeof(SubTimeFrame::Header));

void DplToStfAdapter::visit(SubTimeFrame& pStf, void*)
{
  bool lStfHeaderFound = false;

  if (mMessages.size() < 2) {
    // stf meta messages must be present
    EDDLOG("DPL interface: expected at least 2 messages received={}", mMessages.size());
    mMessages.clear();
    pStf.clear();

    throw std::runtime_error("SubTimeFrame::MessageNumber");
  }

  auto lAddFullSplitPayload = [this, &pStf](std::size_t &idx, const std::size_t count) {

    DDDLOG_RL(10000, "DPL Deserialize: converting full split payload");

    const auto lStopIdx = idx + (2 * count);

    auto &lHdr = mMessages[idx++];
    auto &lData = mMessages[idx++];

    DataHeader *lHdrPtr = reinterpret_cast<DataHeader*>(lHdr->GetData());
    const auto lSubSpec = lHdrPtr->subSpecification;
    const o2hdr::DataIdentifier lDataId = impl::getDataIdentifier(*lHdrPtr);

    // convert to reduced header type
    lHdrPtr->splitPayloadIndex = lHdrPtr->splitPayloadParts;

    auto *lVec = pStf.addStfDataStart(lDataId, lSubSpec, {std::move(lHdr), std::move(lData)});

    for (; idx < mMessages.size() && idx < lStopIdx; idx +=2) {
      pStf.addStfDataAppend(lVec, std::move(mMessages[idx+1]));
    }
  };

  auto lAddReducedSplitPayload = [this, &pStf](std::size_t &idx, const std::size_t count) {

    const auto lStopIdx = idx + (1 + count);

    auto &lHdr = mMessages[idx++];
    auto &lData = mMessages[idx++];

    DataHeader *lHdrPtr = reinterpret_cast<DataHeader*>(lHdr->GetData());
    const auto lSubSpec = lHdrPtr->subSpecification;
    const o2hdr::DataIdentifier lDataId = impl::getDataIdentifier(*lHdrPtr);

    auto *lVec = pStf.addStfDataStart(lDataId, lSubSpec, {std::move(lHdr), std::move(lData)});

    // add the rest of data
    for (; idx < mMessages.size() && idx < lStopIdx; idx++) {
      pStf.addStfDataAppend(lVec, std::move(mMessages[idx]));
    }
  };

  // iterate over all incoming HBFrame data sources
  for (size_t i = 0; i < mMessages.size(); ) {

    const DataHeader *lHdrPtr = reinterpret_cast<o2::header::DataHeader*>(mMessages[i]->GetData());

    // Ordinary (single) message
    if (lHdrPtr->splitPayloadParts <= 1) {
      auto& lHdrMsg = mMessages[i + 0];
      auto& lDataMsg = mMessages[i + 1];

      // check for DD STF header
      if (!lStfHeaderFound && (sizeof(SubTimeFrame::Header) == lDataMsg->GetSize())) {
        if (lHdrMsg->GetSize() < sizeof(o2::header::DataHeader)) {
          EDDLOG("DPL interface: cannot find DataHeader in header stack");
          mMessages.clear();
          pStf.clear();
          throw std::runtime_error("SubTimeFrame::Header::DataHeader");
        }
      }

      // check if StfHeader
      if (gDataDescSubTimeFrame == lHdrPtr->dataDescription) {
        // copy the contents
        std::memcpy(&pStf.mHeader, lDataMsg->GetData(), sizeof(SubTimeFrame::Header));
        lStfHeaderFound = true;
      } else {
        // Insert ordinary (single) message
        const auto lDataId = impl::getDataIdentifier(*lHdrPtr);
        pStf.addStfDataStart(lDataId, lHdrPtr->subSpecification, {std::move(lHdrMsg), std::move(lDataMsg)});
      }

      i += 2;
      continue;
    }

    // split payload message
    if (lHdrPtr->splitPayloadParts > 1) {
      if (lHdrPtr->splitPayloadIndex == 0) {
        // FullHeader Split payload
        lAddFullSplitPayload(i, lHdrPtr->splitPayloadParts);
      } else if (lHdrPtr->splitPayloadIndex == lHdrPtr->splitPayloadParts) {
        // Reduced Split payload type
        lAddReducedSplitPayload(i, lHdrPtr->splitPayloadParts);
      } else {
        EDDLOG_RL(1000, "Invalid split payload header message");
      }
    }
  }

  if (!lStfHeaderFound) {
    throw std::runtime_error("STF receive error: missing SubTimeFrame::Header missing_o2_hdr=" +
      gDataDescSubTimeFrame.as<std::string>());
  }
}

std::unique_ptr<SubTimeFrame> DplToStfAdapter::deserialize_impl()
{
  // NOTE: StfID will be updated from the stf header
  std::unique_ptr<SubTimeFrame> lStf = std::make_unique<SubTimeFrame>(0);

  try {
    // check for EoS message
    if (mMessages.size() == 2) {
      auto lEoSHeader = header::get<o2::framework::SourceInfoHeader*>(mMessages[0]->GetData(), mMessages[0]->GetSize());
      if (lEoSHeader && (lEoSHeader->state == o2::framework::InputChannelState::Completed)) {
        mMessages.clear();
        IDDLOG_RL(1000, "End of stream (EoS) message received.");
        return nullptr;
      }
    }

    lStf->accept(*this);
  } catch (std::runtime_error& e) {
    EDDLOG("SubTimeFrame deserialization failed. reason={}", e.what());
    mMessages.clear();
    return nullptr;
  } catch (std::exception& e) {
    EDDLOG("SubTimeFrame deserialization failed. reason={}", e.what());
    mMessages.clear();
    return nullptr;
  }

  // make sure headers and chunk pointers don't linger
  mMessages.clear();

  // update STF data
  lStf->updateStf();

  return lStf;
}

std::unique_ptr<SubTimeFrame> DplToStfAdapter::deserialize(FairMQParts& pMsgs)
{
  swap(mMessages, pMsgs.fParts);
  pMsgs.fParts.clear();

  return deserialize_impl();
}

std::unique_ptr<SubTimeFrame> DplToStfAdapter::deserialize(FairMQChannel& pChan, bool pLogError)
{
  mMessages.clear();
  const std::int64_t lRet = pChan.Receive(mMessages, 500 /* ms */);

  switch (lRet) {
    case static_cast<std::int64_t>(fair::mq::TransferCode::timeout):
      mMessages.clear();
      return nullptr;
      break;
    case static_cast<std::int64_t>(fair::mq::TransferCode::interrupted):
      if (pLogError) {
        IDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::interrupted");
      }
      mMessages.clear();
      return nullptr;
      break;
    case static_cast<std::int64_t>(fair::mq::TransferCode::error):
      EDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::error err={} errno={} error={}",
        int(lRet), errno, std::string(strerror(errno)));
      mMessages.clear();
      return nullptr;
      break;
    default: // data or zero
      if (lRet > 0) {
        return deserialize_impl();
      } else {
        WDDLOG_RL(1000, "STF receive failed. what=zero_size");
        mMessages.clear();
        return nullptr;
      }
      break;
  }

  assert (false);
  return nullptr;
}

} /* o2::DataDistribution */
