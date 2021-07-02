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
#include <Headers/Stack.h>

namespace o2::DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// StfDplAdapter
////////////////////////////////////////////////////////////////////////////////

void StfToDplAdapter::visit(SubTimeFrame& pStf)
{
  // Pack the Stf header
  o2::header::DataHeader lStfDistDataHeader(
    gDataDescSubTimeFrame,
    o2::header::gDataOriginFLP,
    0, // TODO: subspecification? FLP ID? EPN ID?
    sizeof(SubTimeFrame::Header)
  );

  lStfDistDataHeader.payloadSerializationMethod = gSerializationMethodNone;
  lStfDistDataHeader.tfCounter = pStf.header().mId;
  lStfDistDataHeader.runNumber = pStf.header().mRunNumber;
  lStfDistDataHeader.firstTForbit = pStf.header().mFirstOrbit;

  o2::framework::DataProcessingHeader lDplHeader(pStf.header().mId);

  {
    auto lHdrStack = Stack(lStfDistDataHeader, lDplHeader);

    auto lDataHeaderMsg = mChan.NewMessage(lHdrStack.size());
    if (!lDataHeaderMsg) {
      EDDLOG("Allocation error: Stf DataHeader. size={}", sizeof(DataHeader));
      throw std::bad_alloc();
    }

    std::memcpy(lDataHeaderMsg->GetData(), lHdrStack.data(), lHdrStack.size());

    auto lDataMsg = mChan.NewMessage(sizeof(SubTimeFrame::Header));
    if (!lDataMsg) {
      EDDLOG("Allocation error: Stf::Header. size={}", sizeof(SubTimeFrame::Header));
      throw std::bad_alloc();
    }
    std::memcpy(lDataMsg->GetData(), &pStf.header(), sizeof(SubTimeFrame::Header));

    mMessages.push_back(std::move(lDataHeaderMsg));
    mMessages.push_back(std::move(lDataMsg));
  }

  // Send data in lexicographical order of DataIdentifier + subSpecification
  // for easier binary comparison
  std::vector<EquipmentIdentifier> lEquipIds = pStf.getEquipmentIdentifiers();
  std::sort(std::begin(lEquipIds), std::end(lEquipIds));

  for (const auto& lEquip : lEquipIds) {

    auto& lHBFrameVector = pStf.mData.at(lEquip).at(lEquip.mSubSpecification);

    for (std::size_t i = 0; i < lHBFrameVector.size(); i++) {

      // O2 messages belonging to a single STF:
      //  - DataProcessingHeader::startTime == STF ID
      //  - DataHeader(origin, description, subspecification) can repeat
      //  - DataHeader(origin, description, subspecification, splitPayloadIndex) is unique

      assert(lHBFrameVector[i].getDataHeader().splitPayloadIndex == i);
      assert(lHBFrameVector[i].getDataHeader().splitPayloadParts == lHBFrameVector.size());

      mMessages.push_back(std::move(lHBFrameVector[i].mHeader));
      mMessages.push_back(std::move(lHBFrameVector[i].mData));
    }
    lHBFrameVector.clear();
  }
}

void StfToDplAdapter::inspect() const
{
  // check validity of the TF message sequence
  const auto cMsgSize = mMessages.size() / 2;

  DataHeader::TForbitType lFirstTForbit = ~  DataHeader::TForbitType{0};
  DataHeader::TFCounterType lTfCounter = ~ DataHeader::TFCounterType{0};
  DataHeader::RunNumberType lRunNumber = ~ DataHeader::RunNumberType{0};

  o2::framework::DataProcessingHeader::StartTime lProcStart = ~ o2::framework::DataProcessingHeader::StartTime{0};

  for (size_t lIdx = 0; lIdx < cMsgSize; ) {
    const DataHeader *lDh = reinterpret_cast<DataHeader*>(mMessages[lIdx*2]->GetData());

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

    const auto *lProcHdr = o2::header::get<o2::framework::DataProcessingHeader*>(mMessages[lIdx*2]->GetData(), mMessages[lIdx*2]->GetSize());

    if (lProcStart == ~ o2::framework::DataProcessingHeader::StartTime{0}) {
      lProcStart = lProcHdr->startTime;
    } else {
      if (lProcStart != lProcHdr->startTime) {
        EDDLOG("DPL output: dpl proc header start is different first={} != current={}", lProcStart, lProcHdr->startTime);
      }
    }

    const auto& [ lOrigin, lType, lSubSpec ] = std::tie( lDh->dataOrigin, lDh->dataDescription, lDh->subSpecification );

    if (lDh->splitPayloadParts == 1) {
      // TODO: this combination of origin cannot appear again!
      lIdx++;
      continue;
    }

    // check we have enough messsages
    if (lDh->splitPayloadParts > cMsgSize - lIdx ) {
      EDDLOG("DPL output: the multi-part is too short for the split-payload message. "
        "splitpayload_size={} tf_available={} tf_total={}", lDh->splitPayloadParts, (cMsgSize - lIdx), cMsgSize);
      break;
    }

    // check the first message
    if (lDh->splitPayloadIndex != 0) {
      EDDLOG("DPL output: index of the first split-payload message is invalid. index={}",
        lDh->splitPayloadIndex);
      break;
    }

    // check all split parts
    for (size_t lSplitI = 1; lSplitI < lDh->splitPayloadParts; lSplitI++) {
      const DataHeader *lSplitDh = reinterpret_cast<DataHeader*>(mMessages[(lIdx+lSplitI)*2]->GetData());

      // check origin
      if (lOrigin != lSplitDh->dataOrigin || lType != lSplitDh->dataDescription || lSubSpec != lSplitDh->subSpecification) {
        EDDLOG("DPL output: origin of the split-payload message is invalid. "
          "[0]=<{}{}{}> [{}]=<{}{}{}>", lOrigin.str, lType.str, lSubSpec,
          lSplitI, lSplitDh->dataOrigin.str, lSplitDh->dataDescription.str, lSplitDh->subSpecification);
        break;
      }

      // check the count value
      if (lDh->splitPayloadParts != lSplitDh->splitPayloadParts) {
        EDDLOG("DPL output: number of payload parts in split-payload message invalid. "
          "split_pos={} number={} original_number={}", lSplitI, lSplitDh->splitPayloadParts, lDh->splitPayloadParts);
        break;
      }

      // check the index value
      if (lSplitDh->splitPayloadIndex != lSplitI) {
        EDDLOG("DPL output: index of the split-payload message is invalid. "
          "split_pos={} != index={} parts_count={}", lSplitI, lSplitDh->splitPayloadIndex, lDh->splitPayloadParts);
        break;
      }
    }

    // advance to the next o2 message
    // TODO: record the origin
    lIdx += lDh->splitPayloadParts;
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




////////////////////////////////////////////////////////////////////////////////
/// DplToStfAdapter
////////////////////////////////////////////////////////////////////////////////

static const o2::header::DataHeader gStfDistDataHeader(
  gDataDescSubTimeFrame,
  o2::header::gDataOriginFLP,
  0, // TODO: subspecification? FLP ID? EPN ID?
  sizeof(SubTimeFrame::Header));

void DplToStfAdapter::visit(SubTimeFrame& pStf)
{
  bool lStfHeaderFound = false;

  if (mMessages.size() < 2) {
    // stf meta messages must be present
    EDDLOG("DPL interface: expected at least 2 messages received={}", mMessages.size());
    mMessages.clear();
    pStf.clear();

    throw std::runtime_error("SubTimeFrame::MessageNumber");
  }

  if (mMessages.size() % 2 != 0) {
    // stf meta messages must be even
    EDDLOG("DPL interface: expected even number of messages received={}", mMessages.size());
    mMessages.clear();
    pStf.clear();

    throw std::runtime_error("SubTimeFrame::MessageNumber");
  }

  // iterate over all incoming HBFrame data sources
  for (size_t i = 0; i < mMessages.size(); i += 2) {

    auto& lHdrMsg = mMessages[i + 0];
    auto& lDataMsg = mMessages[i + 1];

    if (!lStfHeaderFound && (sizeof(SubTimeFrame::Header) == lDataMsg->GetSize())) {
      // this is too slow to do here
      // const DataHeader* lStfDataHdr = o2::header::get<DataHeader*>(lHdrMsg->GetData(), lHdrMsg->GetSize());
      // NOTE: DataHeader MUST be at position 0
      const DataHeader* lStfDataHdr = reinterpret_cast<o2::header::DataHeader*>(lHdrMsg->GetData());
      if (lHdrMsg->GetSize() < sizeof(o2::header::DataHeader)) {
        EDDLOG("DPL interface: cannot find DataHeader in header stack");
        mMessages.clear();
        pStf.clear();
        throw std::runtime_error("SubTimeFrame::Header::DataHeader");
      }

      // check if StfHeader
      if (gDataDescSubTimeFrame == lStfDataHdr->dataDescription) {
        // copy the contents
        std::memcpy(&pStf.mHeader, lDataMsg->GetData(), sizeof(SubTimeFrame::Header));
        lStfHeaderFound = true;
        continue;
      }
    }

    // add the data to the STF
    pStf.addStfData({ std::move(lHdrMsg), std::move(lDataMsg) });
  }
}

std::unique_ptr<SubTimeFrame> DplToStfAdapter::deserialize_impl()
{
  // NOTE: StfID will be updated from the stf header
  std::unique_ptr<SubTimeFrame> lStf = std::make_unique<SubTimeFrame>(0);

  try {
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
