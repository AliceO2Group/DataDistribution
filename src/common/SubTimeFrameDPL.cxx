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

namespace o2
{
namespace DataDistribution
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

  o2::framework::DataProcessingHeader lDplHeader(pStf.header().mId);

  {
    auto lHdrStack = Stack(lStfDistDataHeader, lDplHeader);

    auto lDataHeaderMsg = mChan.NewMessage(lHdrStack.size());
    if (!lDataHeaderMsg) {
      DDLOG(fair::Severity::ERROR) << "Allocation error: Stf DataHeader::size: " << sizeof(DataHeader);
      throw std::bad_alloc();
    }

    std::memcpy(lDataHeaderMsg->GetData(), lHdrStack.data(), lHdrStack.size());

    auto lDataMsg = mChan.NewMessage(sizeof(SubTimeFrame::Header));
    if (!lDataMsg) {
      DDLOG(fair::Severity::ERROR) << "Allocation error: Stf::Header::size: " << sizeof(SubTimeFrame::Header);
      throw std::bad_alloc();
    }
    std::memcpy(lDataMsg->GetData(), &pStf.header(), sizeof(SubTimeFrame::Header));

    mMessages.emplace_back(std::move(lDataHeaderMsg));
    mMessages.emplace_back(std::move(lDataMsg));
  }

  for (auto& lDataIdentMapIter : pStf.mData) {

    auto &lSubSpecDataMap = lDataIdentMapIter.second;

    for (auto& lSubSpecMapIter : lSubSpecDataMap) {

      auto & lHBFrameVector = lSubSpecMapIter.second;

      for (uint64_t i = 0; i < lHBFrameVector.size(); i++) {

        // O2 messages belonging to a single STF:
        //  - DataProcessingHeader::startTime == STF ID
        //  - DataHeader(origin, description, subspecification) can repeat
        //  - DataHeader(origin, description, subspecification, splitPayloadIndex) is unique

        assert(lHBFrameVector[i].getDataHeader().splitPayloadIndex == i);
        assert(lHBFrameVector[i].getDataHeader().splitPayloadParts == lHBFrameVector.size());

        mMessages.emplace_back(std::move(lHBFrameVector[i].mHeader));
        mMessages.emplace_back(std::move(lHBFrameVector[i].mData));
      }
      lHBFrameVector.clear();
    }
  }
}

void StfToDplAdapter::sendToDpl(std::unique_ptr<SubTimeFrame>&& pStf)
{
  mMessages.clear();
  pStf->accept(*this);

#if 0
  DDLOG(fair::Severity::DEBUG) << "Content of the Stf:";
  uint64_t lMsgIdx = 0;
  for (auto lM = mMessages.cbegin(); lM != mMessages.cend(); ) {
    DDLOG(fair::Severity::DEBUG) << "  o2: message " << lMsgIdx++;
    o2::header::hexDump("o2 header", (*lM)->GetData(), (*lM)->GetSize());
    lM++;
    o2::header::hexDump("o2 payload", (*lM)->GetData(), std::clamp((*lM)->GetSize(), std::size_t(0), std::size_t(256)) );
    lM++;
  }
#endif

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
  if (mMessages.size() < 2) {
    // stf meta messages must be present
    DDLOGF(fair::Severity::ERROR, "DPL interface: expected at least 2 messages received={}", mMessages.size());
    mMessages.clear();
    pStf.clear();

    throw std::runtime_error("SubTimeFrame::MessageNumber");
  }

  if (mMessages.size() % 2 != 0) {
    // stf meta messages must be even
    DDLOGF(fair::Severity::ERROR, "DPL interface: expected even number of messages received={}", mMessages.size());
    mMessages.clear();
    pStf.clear();

    throw std::runtime_error("SubTimeFrame::MessageNumber");
  }

  // iterate over all incoming HBFrame data sources
  for (size_t i = 0; i < mMessages.size(); i += 2) {

    auto& lHdrMsg = mMessages[i + 0];
    auto& lDataMsg = mMessages[i + 1];

    const DataHeader* lStfDataHdr = o2::header::get<DataHeader*>(lHdrMsg->GetData(), lHdrMsg->GetSize());

    if (!lStfDataHdr) {
      DDLOGF(fair::Severity::ERROR, "DPL interface: cannot find DataHeader in header stack");
      mMessages.clear();
      pStf.clear();
      throw std::runtime_error("SubTimeFrame::Header::DataHeader");
    }

    // check if StfHeader
    if (gStfDistDataHeader == *lStfDataHdr) {

      if (sizeof(SubTimeFrame::Header) != lDataMsg->GetSize()) {
        DDLOGF(fair::Severity::ERROR, "DPL interface: Stf Header size does not match expected={} received={}",
          sizeof(SubTimeFrame::Header), lDataMsg->GetSize());

        mMessages.clear();
        pStf.clear();
        throw std::runtime_error("SubTimeFrame::Header::SizeError");
      }

      std::memcpy(&pStf.mHeader, lDataMsg->GetData(), sizeof(SubTimeFrame::Header));
    } else {
      // add the data to the STF

      pStf.addStfData({ std::move(lHdrMsg), std::move(lDataMsg) });
    }
  }
}

std::unique_ptr<SubTimeFrame> DplToStfAdapter::deserialize_impl()
{
  // NOTE: StfID will be updated from the stf header
  std::unique_ptr<SubTimeFrame> lStf = std::make_unique<SubTimeFrame>(0);

  try {
    lStf->accept(*this);
  } catch (std::runtime_error& e) {
    DDLOGF(fair::Severity::ERROR, "SubTimeFrame deserialization failed. reason={}", e.what());
    mMessages.clear();
    return nullptr;
  } catch (std::exception& e) {
    DDLOGF(fair::Severity::ERROR, "SubTimeFrame deserialization failed. reason={}", e.what());
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

std::unique_ptr<SubTimeFrame> DplToStfAdapter::deserialize(FairMQChannel& pChan)
{
  mMessages.clear();
  const std::int64_t ret = pChan.Receive(mMessages, 500 /* ms */);

  // timeout ?
  if (ret == -2) {
    return nullptr;
  }

  if (ret < 0) {
    { // rate-limited LOG: print stats once per second
      static unsigned long floodgate = 0;
      if (floodgate++ % 10 == 0) {
        DDLOGF(fair::Severity::ERROR, "STF receive failed err={} errno={} error={}",
               ret, errno, std::string(strerror(errno)));
      }
    }

    mMessages.clear();
    return nullptr;
  }

  return deserialize_impl();
}

}
} /* o2::DataDistribution */
