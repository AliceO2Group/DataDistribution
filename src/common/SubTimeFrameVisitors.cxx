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

#include "SubTimeFrameVisitors.h"
#include "SubTimeFrameDataModel.h"
#include "DataModelUtils.h"

#include "DataDistLogger.h"

#include <stdexcept>
#include <vector>
#include <deque>
#include <new>
#include <memory>
#include <algorithm>

namespace o2::DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// InterleavedHdrDataSerializer
////////////////////////////////////////////////////////////////////////////////

static const o2::header::DataHeader gStfDistDataHeader(
  gDataDescSubTimeFrame,
  o2::header::gDataOriginFLP,
  0, // TODO: subspecification? FLP ID? EPN ID?
  sizeof(SubTimeFrame::Header));

void InterleavedHdrDataSerializer::visit(SubTimeFrame& pStf)
{
  // Pack the Stf header
  auto lDataHeaderMsg = mChan.NewMessage(sizeof(DataHeader));
  if (!lDataHeaderMsg) {
    EDDLOG("Allocation error: Stf DataHeader. size={}", sizeof(DataHeader));
    throw std::bad_alloc();
  }
  DataHeader *lHdrPtr = reinterpret_cast<DataHeader*>(lDataHeaderMsg->GetData());
  std::memcpy(lHdrPtr, &gStfDistDataHeader, sizeof(DataHeader));
  lHdrPtr->firstTForbit = pStf.header().mFirstOrbit;
  lHdrPtr->runNumber = pStf.header().mRunNumber;
  lHdrPtr->payloadSerializationMethod = gSerializationMethodNone;

  auto lDataMsg = mChan.NewMessage(sizeof(SubTimeFrame::Header));
  if (!lDataMsg) {
    EDDLOG("Allocation error: Stf::Header. size={}", sizeof(SubTimeFrame::Header));
    throw std::bad_alloc();
  }
  std::memcpy(lDataMsg->GetData(), &pStf.header(), sizeof(SubTimeFrame::Header));

  mMessages.push_back(std::move(lDataHeaderMsg));
  mMessages.push_back(std::move(lDataMsg));

  for (auto& lDataIdentMapIter : pStf.mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {

        if (!lStfDataIter.mHeader) {
          EDDLOG_RL(1000, "BUG: InterleavedHdrDataSerializer: O2 headers must not be removed before this call!");
          continue;
        }

        mMessages.push_back(std::move(lStfDataIter.mHeader));

        if (lStfDataIter.mData->GetSize() == 0) {
          EDDLOG("Sending STF data payload with zero size");
        }

        mMessages.push_back(std::move(lStfDataIter.mData));
      }
    }
  }

  pStf.mData.clear();
  pStf.mHeader = SubTimeFrame::Header();
}

void InterleavedHdrDataSerializer::serialize(std::unique_ptr<SubTimeFrame>&& pStf)
{
  mMessages.clear();
  pStf->accept(*this);

  mChan.Send(mMessages);

  // make sure headers and chunk pointers don't linger
  mMessages.clear();
}

////////////////////////////////////////////////////////////////////////////////
/// InterleavedHdrDataDeserializer
////////////////////////////////////////////////////////////////////////////////
void InterleavedHdrDataDeserializer::visit(SubTimeFrame& pStf)
{
  assert(mMessages.size() >= 2); // stf meta messages must be present
  assert(mMessages.size() % 2 == 0);

  // header
  DataHeader lStfDataHdr;
  std::memcpy(&lStfDataHdr, mMessages[0]->GetData(), sizeof(DataHeader));
  // verify the stf DataHeader
  if (!(gStfDistDataHeader == lStfDataHdr)) {
   WDDLOG("Receiving bad SubTimeFrame::Header::DataHeader message");
    throw std::runtime_error("SubTimeFrame::Header::DataHeader");
  }
  // copy the header
  std::memcpy(&pStf.mHeader, mMessages[1]->GetData(), sizeof(SubTimeFrame::Header));

  // iterate over all incoming HBFrame data sources
  for (size_t i = 2; i < mMessages.size(); i += 2) {

    auto &lDataMsg = mMessages[i + 1];

    if (lDataMsg->GetSize() == 0) {
      EDDLOG("Received STF data payload with zero size");
    }

    pStf.addStfData({ std::move(mMessages[i]), std::move(lDataMsg) });
  }
}

std::unique_ptr<SubTimeFrame> InterleavedHdrDataDeserializer::deserialize(FairMQChannel& pChan, bool pLogError)
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

std::unique_ptr<SubTimeFrame> InterleavedHdrDataDeserializer::deserialize(FairMQParts& pMsgs)
{
  swap(mMessages, pMsgs.fParts);
  pMsgs.fParts.clear();

  return deserialize_impl();
}

std::unique_ptr<SubTimeFrame> InterleavedHdrDataDeserializer::deserialize_impl()
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

  return lStf;
}



////////////////////////////////////////////////////////////////////////////////
/// CoalescedHdrDataSerializer
////////////////////////////////////////////////////////////////////////////////

void CoalescedHdrDataSerializer::visit(SubTimeFrame& pStf)
{
  // Pack the Stf header
  auto lDataHeaderMsg = mChan.NewMessage(sizeof(DataHeader));
  if (!lDataHeaderMsg) {
    EDDLOG("Allocation error: Stf DataHeader. size={}", sizeof(DataHeader));
    throw std::bad_alloc();
  }

  DataHeader *lHdrPtr = reinterpret_cast<DataHeader*>(lDataHeaderMsg->GetData());
  std::memcpy(lHdrPtr, &gStfDistDataHeader, sizeof(DataHeader));
  lHdrPtr->firstTForbit = pStf.header().mFirstOrbit;
  lHdrPtr->runNumber = pStf.header().mRunNumber;
  lHdrPtr->payloadSerializationMethod = gSerializationMethodNone;

  auto lDataMsg = mChan.NewMessage(sizeof(SubTimeFrame::Header));
  if (!lDataMsg) {
    EDDLOG("Allocation error: Stf::Header. size={}", sizeof(SubTimeFrame::Header));
    throw std::bad_alloc();
  }
  std::memcpy(lDataMsg->GetData(), &pStf.header(), sizeof(SubTimeFrame::Header));

  // NOTE: header vector has 2 messages more than the data vector
  mHdrs.push_back(std::move(lDataHeaderMsg));
  mHdrs.push_back(std::move(lDataMsg));

  for (auto& lDataIdentMapIter : pStf.mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {
        mHdrs.push_back(std::move(lStfDataIter.mHeader));
        mData.push_back(std::move(lStfDataIter.mData));
      }
    }
  }

  pStf.mData.clear();
  pStf.mHeader = SubTimeFrame::Header();
}

void CoalescedHdrDataSerializer::serialize(std::unique_ptr<SubTimeFrame>&& pStf)
{
  mHdrs.clear();
  mData.clear();

  pStf->accept(*this);

  // coalesce the headers
  const std::size_t lHdrSize = std::accumulate(mHdrs.begin(), mHdrs.end(), std::size_t(0),
    [](const std::size_t v, const FairMQMessagePtr&h) {
      // check if we have to fake a removed header
      if (h == nullptr) {
        return v + sizeof(DataHeader);
      } else {
        return v + h->GetSize();
      }
    }
  );

  const std::size_t lTotalHdrSize = (mHdrs.size() * sizeof(header_info)) + lHdrSize;

  DDLOGF_GRL(5000, DataDistSeverity::debug, "CoalescedHdrDataSerializer: headers={} coalesced_size={}",
    mHdrs.size(), lTotalHdrSize);

  auto lFullHdrMsg = mChan.NewMessage(lTotalHdrSize);
  if (!lFullHdrMsg) {
    EDDLOG("Allocation error: Stf::CoalescedHeader. size={}", sizeof(lTotalHdrSize));
    throw std::bad_alloc();
  }

  std::size_t lInfoOff = 0;
  std::size_t lHdrOff = mHdrs.size() * sizeof(header_info);
  char* lFullHdrMsgAddr = reinterpret_cast<char*>(lFullHdrMsg->GetData());

  // coalesce all headers into a single message
  // keep a reference to the last non-null header
  // update index on the fly
  DataHeader lLastDataHeader;
  DataHeader::SplitPayloadIndexType lLastDataHeaderIdx = 0;

  for (std::size_t i = 0; i < mHdrs.size(); i++) {

    const auto &lHdr = mHdrs[i];
    const header_info lHdrInfo = { lHdrOff, (lHdr) ? lHdr->GetSize() : sizeof(DataHeader) };

    // copy the header descriptor
    std::memcpy(lFullHdrMsgAddr + lInfoOff, &lHdrInfo, sizeof(header_info));

    if (lHdr) {
      // save the header to reconstruct the missing redundant split-payload headers
      if (lHdr->GetSize() >= sizeof(DataHeader)) {
        std::memcpy(&lLastDataHeader, reinterpret_cast<const char*>(lHdr->GetData()), sizeof(DataHeader));
        lLastDataHeaderIdx = 0;
      }

      // serialize the header to the coalesced message
      std::memcpy(lFullHdrMsgAddr + lHdrOff, reinterpret_cast<const char*>(lHdr->GetData()), lHdrInfo.len);
    } else {
      // make sure the desc is RAW?
      if (lLastDataHeader.dataDescription != gDataDescriptionRawData) {
        EDDLOG_RL(1000, "BUG: CoalescedHdrDataSerializer: O2 trailer header must be RAWDATA!");
      }

      // update the payload index and size
      lLastDataHeaderIdx += 1;
      lLastDataHeader.splitPayloadIndex = lLastDataHeaderIdx;
      lLastDataHeader.payloadSize = mData[i-2]->GetSize(); // indexing: see the visit method

      // serialize the header to the coalesced message
      std::memcpy(lFullHdrMsgAddr + lHdrOff, &lLastDataHeader, sizeof (DataHeader));
    }

    lInfoOff += sizeof(header_info);
    lHdrOff += lHdrInfo.len;

    assert (lInfoOff <= mHdrs.size() * sizeof(header_info));
    assert (lHdrOff <= lTotalHdrSize);
  }
  assert (lInfoOff == mHdrs.size() * sizeof(header_info));
  assert (lHdrOff == lTotalHdrSize);

  // add it to data messages for sending
  mData.push_back(std::move(lFullHdrMsg));
  mHdrs.clear();

  // send the data + coslesced headers
  while (mRunning && (static_cast<std::int64_t>(fair::mq::TransferCode::timeout) == mChan.Send(mData, 500))) { }

  // make sure headers and chunk pointers don't linger
  mData.clear();
}

////////////////////////////////////////////////////////////////////////////////
/// CoalescedHdrDataDeserializer
////////////////////////////////////////////////////////////////////////////////
void CoalescedHdrDataDeserializer::visit(SubTimeFrame& pStf)
{
  assert(mHdrs.size() >= 2); // stf meta messages must be present
  assert(mData.size() == mHdrs.size()-2); // we pack 2transient messages in the begining of headers

  // header
  DataHeader lStfDataHdr;
  std::memcpy(&lStfDataHdr, mHdrs[0]->GetData(), sizeof(DataHeader));
  // verify the stf DataHeader
  if (!(gStfDistDataHeader == lStfDataHdr)) {
    WDDLOG("Receiving bad SubTimeFrame::Header::DataHeader message");
    throw std::runtime_error("SubTimeFrame::Header::DataHeader");
  }
  // copy the header
  std::memcpy(&pStf.mHeader, mHdrs[1]->GetData(), sizeof(SubTimeFrame::Header));

  // iterate over all incoming HBFrame data sources
  for (size_t i = 0; i < mData.size(); i += 1) {

    auto &lHdrMsg = mHdrs[i+2];
    auto &lDataMsg = mData[i];

    if (lDataMsg->GetSize() == 0) {
      EDDLOG("Received STF data payload with zero size");
    }

#if !defined(NDEBUG)
    // make sure header payload size matches
     DataHeader *lHdrPtr = reinterpret_cast<DataHeader*>(lHdrMsg->GetData());
     if (lHdrPtr->payloadSize != lDataMsg->GetSize()) {
       EDDLOG_RL(1000, "BUG: Header payloadSize does not match the data size. hdr_size={} data_size={}", lHdrPtr->payloadSize, lDataMsg->GetSize());
     }
#endif

    pStf.addStfData({ std::move(lHdrMsg), std::move(lDataMsg) });
  }
}

std::unique_ptr<SubTimeFrame> CoalescedHdrDataDeserializer::deserialize(FairMQChannel& pChan, bool pLogError)
{
  mHdrs.clear();
  mData.clear();

  const std::int64_t lRet = pChan.Receive(mData, 500 /* ms */);

  switch (lRet) {
    case static_cast<std::int64_t>(fair::mq::TransferCode::timeout):
      mData.clear();
      return nullptr;
      break;
    case static_cast<std::int64_t>(fair::mq::TransferCode::interrupted):
      if (pLogError) {
        IDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::interrupted");
      }
      mData.clear();
      return nullptr;
      break;
    case static_cast<std::int64_t>(fair::mq::TransferCode::error):
      EDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::error err={} errno={} error={}",
        int(lRet), errno, std::string(strerror(errno)));
      mData.clear();
      return nullptr;
      break;
    default: // data or zero
      if (lRet > 0) {
        return deserialize_impl();
      } else {
        WDDLOG_RL(1000, "STF receive failed. what=zero_size");
        mData.clear();
        return nullptr;
      }
      break;
  }

  assert (false);
  return nullptr;
}

std::unique_ptr<SubTimeFrame> CoalescedHdrDataDeserializer::deserialize(std::vector<FairMQMessagePtr>& pMsgs)
{
  mHdrs.clear();
  mData.clear();

  swap(mData, pMsgs);
  pMsgs.clear();

  return deserialize_impl();
}

std::unique_ptr<SubTimeFrame> CoalescedHdrDataDeserializer::deserialize_impl()
{
  // NOTE: StfID will be updated from the stf header
  std::unique_ptr<SubTimeFrame> lStf = std::make_unique<SubTimeFrame>(0);
  try {
    // recreate header messages
    mHdrs.clear();

    const auto lCoalescedHdr = std::move(*mData.rbegin());
    mData.pop_back();

    // we pack 2 transient stf header messages into Hdrs
    const auto lExpectedMsgs = mData.size() + 2;

    DDLOGF_GRL(5000, DataDistSeverity::debug, "CoalescedHdrDataDeserializer: headers={} coalesced_size={}",
      lExpectedMsgs, lCoalescedHdr->GetSize());

    std::size_t lInfoOff = 0;
    std::size_t lHdrOff = lExpectedMsgs * sizeof(CoalescedHdrDataSerializer::header_info);
    char* lFullHdrMsgAddr = reinterpret_cast<char*>(lCoalescedHdr->GetData());

    // sanity checking
    if (lCoalescedHdr->GetSize() < lExpectedMsgs * sizeof(CoalescedHdrDataSerializer::header_info)) {
      EDDLOG("CoalescedHdrDataDeserializer: packed header message too small. size={} min_expected_size={}",
        lCoalescedHdr->GetSize(), (lExpectedMsgs * sizeof(CoalescedHdrDataSerializer::header_info)));
      throw std::runtime_error("CoalescedHdrDataDeserializer::HeaderSize too small");
    }

    // unpack coalesced headers
    for (unsigned m = 0; m < lExpectedMsgs; m++) {
      CoalescedHdrDataSerializer::header_info lHdrInfo;
      std::memcpy(&lHdrInfo, lFullHdrMsgAddr + lInfoOff, sizeof(CoalescedHdrDataSerializer::header_info));

      if (lHdrInfo.len > (100 << 10)) {
        WDDLOG("CoalescedHdrDataDeserializer: unpacked header size is too large. size={}", lHdrInfo.len);
      }

      if (lHdrInfo.start != (lHdrOff)) {
        EDDLOG("CoalescedHdrDataDeserializer: header unpacking failed. msg_idx={} offset_meta={} offset_unpacking={}",
          m, lHdrInfo.start, lHdrOff);
        throw std::runtime_error("CoalescedHdrDataDeserializer::Deserializing failed");
      }

      auto lNewHdr = mTfBld.newHeaderMessage(lFullHdrMsgAddr + lHdrOff, lHdrInfo.len);

      lInfoOff += sizeof(CoalescedHdrDataSerializer::header_info);
      lHdrOff += lHdrInfo.len;

      mHdrs.push_back(std::move(lNewHdr));
    }

    lStf->accept(*this);

  } catch (std::runtime_error& e) {
    EDDLOG("SubTimeFrame deserialization failed. reason={}", e.what());
    mHdrs.clear();
    mData.clear();
    return nullptr;
  } catch (std::exception& e) {
    EDDLOG("SubTimeFrame deserialization failed. reason={}", e.what());
    mHdrs.clear();
    mData.clear();
    return nullptr;
  }

  // make sure headers and chunk pointers don't linger
  mHdrs.clear();
  mData.clear();

  return lStf;
}


SubTimeFrame::Header CoalescedHdrDataDeserializer::peek_tf_header(const std::vector<FairMQMessagePtr>& pMsgs) const
{
  try {
    // recreate header messages
    const auto &lCoalescedHdr = *pMsgs.rbegin();

    // we pack 2 transient stf header messages into Hdrs and 1 coalesced header msg
    const auto lExpectedMsgs = pMsgs.size() + 1;

    std::size_t lInfoOff = 0;
    std::size_t lHdrOff = lExpectedMsgs * sizeof(CoalescedHdrDataSerializer::header_info);
    char* lFullHdrMsgAddr = reinterpret_cast<char*>(lCoalescedHdr->GetData());

    // sanity checking
    if (lCoalescedHdr->GetSize() < lExpectedMsgs * sizeof(CoalescedHdrDataSerializer::header_info)) {
      EDDLOG("CoalescedHdrDataDeserializer: packed header message too small. size={} min_expected_size={}",
        lCoalescedHdr->GetSize(), (lExpectedMsgs * sizeof(CoalescedHdrDataSerializer::header_info)));
      throw std::runtime_error("CoalescedHdrDataDeserializer::HeaderSize too small");
    }

    // unpack coalesced headers
    SubTimeFrame::Header lStfHeader;

    CoalescedHdrDataSerializer::header_info lHdrInfo;
    // 1st message is the data header
    std::memcpy(&lHdrInfo, lFullHdrMsgAddr + lInfoOff, sizeof(CoalescedHdrDataSerializer::header_info));

    if (lHdrInfo.len > (100 << 10)) {
      WDDLOG("CoalescedHdrDataDeserializer: unpacked header size is too large. size={}", lHdrInfo.len);
    }

    if (lHdrInfo.start != (lHdrOff)) {
      EDDLOG("CoalescedHdrDataDeserializer: header unpacking failed. msg_idx={} offset_meta={} offset_unpacking={}",
        0, lHdrInfo.start, lHdrOff);
      throw std::runtime_error("CoalescedHdrDataDeserializer::Deserializing failed");
    }

    // auto lNewHdr = mTfBld.newHeaderMessage(lFullHdrMsgAddr + lHdrOff, lHdrInfo.len);

    lInfoOff += sizeof(CoalescedHdrDataSerializer::header_info);
    lHdrOff += lHdrInfo.len;


    // 2nd message is the STF header
    std::memcpy(&lHdrInfo, lFullHdrMsgAddr + lInfoOff, sizeof(CoalescedHdrDataSerializer::header_info));

    if (lHdrInfo.len > (100 << 10)) {
      WDDLOG("CoalescedHdrDataDeserializer: unpacked header size is too large. size={}", lHdrInfo.len);
    }

    if (lHdrInfo.start != (lHdrOff)) {
      EDDLOG("CoalescedHdrDataDeserializer: header unpacking failed. msg_idx={} offset_meta={} offset_unpacking={}",
        1, lHdrInfo.start, lHdrOff);
      throw std::runtime_error("CoalescedHdrDataDeserializer::Deserializing failed");
    }

    // auto lNewHdr = mTfBld.newHeaderMessage(lFullHdrMsgAddr + lHdrOff, lHdrInfo.len);
    if (lHdrInfo.len != sizeof (SubTimeFrame::Header)) {
      EDDLOG("CoalescedHdrDataDeserializer: header unpacking failed. Expected StfHeader_size={} msg_idx=1 offset_meta={} offset_unpacking={} hdr_size={}",
        sizeof (SubTimeFrame::Header), lHdrInfo.start, lHdrOff, lHdrInfo.len);
      throw std::runtime_error("CoalescedHdrDataDeserializer::Deserializing failed");
    }

    memcpy(&lStfHeader, lFullHdrMsgAddr + lHdrOff, lHdrInfo.len);

    return lStfHeader;

  } catch (std::runtime_error& e) {
    EDDLOG("SubTimeFrame deserialization failed. reason={}", e.what());
  } catch (std::exception& e) {
    EDDLOG("SubTimeFrame deserialization failed. reason={}", e.what());
  }

  return SubTimeFrame::Header();
}

// copy all messages into the data region, and update the vector
bool CoalescedHdrDataDeserializer::copy_to_region(std::vector<FairMQMessagePtr>& pMsgs /* in/out */)
{

  for (std::size_t idx = 0; idx < pMsgs.size(); idx++) {
    const auto lSize = pMsgs[idx]->GetSize();

    auto lMsgCopy = mTfBld.newDataMessage(reinterpret_cast<const char*>(pMsgs[idx]->GetData()), lSize);

    if (lMsgCopy) {
      pMsgs[idx] = std::move(lMsgCopy);
    } else {
      WDDLOG_GRL(1000, "CoalescedHdrDataDeserializer::copy_to_region: DataRegion allocation failed.");
    }
  }

  return true;
}


} /* o2::DataDistribution */
