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

namespace o2
{
namespace DataDistribution
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
  std::memcpy(lDataHeaderMsg->GetData(), &gStfDistDataHeader, sizeof(DataHeader));
  reinterpret_cast<DataHeader*>(lDataHeaderMsg->GetData())->firstTForbit = pStf.header().mFirstOrbit;
  reinterpret_cast<DataHeader*>(lDataHeaderMsg->GetData())->runNumber = pStf.header().mRunNumber;

  reinterpret_cast<DataHeader*>(lDataHeaderMsg->GetData())->payloadSerializationMethod = gSerializationMethodNone;

  auto lDataMsg = mChan.NewMessage(sizeof(SubTimeFrame::Header));
  if (!lDataMsg) {
    EDDLOG("Allocation error: Stf::Header. size={}", sizeof(SubTimeFrame::Header));
    throw std::bad_alloc();
  }
  std::memcpy(lDataMsg->GetData(), &pStf.header(), sizeof(SubTimeFrame::Header));

  mMessages.emplace_back(std::move(lDataHeaderMsg));
  mMessages.emplace_back(std::move(lDataMsg));

  for (auto& lDataIdentMapIter : pStf.mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {
        mMessages.emplace_back(std::move(lStfDataIter.mHeader));

        if (lStfDataIter.mData->GetSize() == 0) {
          EDDLOG("Sending STF data payload with zero size");
        }

        mMessages.emplace_back(std::move(lStfDataIter.mData));
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

std::unique_ptr<SubTimeFrame> InterleavedHdrDataDeserializer::deserialize(FairMQChannel& pChan)
{
  const std::int64_t ret = pChan.Receive(mMessages, 500 /* ms */);

  // timeout ?
  if (ret == -2) {
     return nullptr;
  }

  if (ret < 0) {
    DDLOGF_RL(1000, DataDistSeverity::error, "STF receive failed err={} errno={} error={}", ret, errno,
      std::string(strerror(errno)));

    mMessages.clear();
    return nullptr;
  }

  return deserialize_impl();
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
  std::memcpy(lDataHeaderMsg->GetData(), &gStfDistDataHeader, sizeof(DataHeader));
  reinterpret_cast<DataHeader*>(lDataHeaderMsg->GetData())->firstTForbit = pStf.header().mFirstOrbit;
  reinterpret_cast<DataHeader*>(lDataHeaderMsg->GetData())->runNumber = pStf.header().mRunNumber;
  reinterpret_cast<DataHeader*>(lDataHeaderMsg->GetData())->payloadSerializationMethod = gSerializationMethodNone;

  auto lDataMsg = mChan.NewMessage(sizeof(SubTimeFrame::Header));
  if (!lDataMsg) {
    EDDLOG("Allocation error: Stf::Header. size={}", sizeof(SubTimeFrame::Header));
    throw std::bad_alloc();
  }
  std::memcpy(lDataMsg->GetData(), &pStf.header(), sizeof(SubTimeFrame::Header));

  mHdrs.emplace_back(std::move(lDataHeaderMsg));
  mHdrs.emplace_back(std::move(lDataMsg));

  for (auto& lDataIdentMapIter : pStf.mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {
        mHdrs.emplace_back(std::move(lStfDataIter.mHeader));
        mData.emplace_back(std::move(lStfDataIter.mData));
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
    [](const std::size_t v, const FairMQMessagePtr&h) { return v + h->GetSize(); }
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
  for (const auto &lHdr : mHdrs) {

    const header_info lHdrInfo = { lHdrOff, lHdr->GetSize() };

    std::memcpy(lFullHdrMsgAddr + lInfoOff, &lHdrInfo, sizeof(header_info));
    std::memcpy(lFullHdrMsgAddr + lHdrOff, reinterpret_cast<const char*>(lHdr->GetData()), lHdrInfo.len);

    lInfoOff += sizeof(header_info);
    lHdrOff += lHdrInfo.len;

    assert (lInfoOff <= mHdrs.size() * sizeof(header_info));
    assert (lHdrOff <= lTotalHdrSize);
  }
  assert (lInfoOff == mHdrs.size() * sizeof(header_info));
  assert (lHdrOff == lTotalHdrSize);

  // add it to data messages for sending
  mData.emplace_back(std::move(lFullHdrMsg));
  mHdrs.clear();

  // send the data + coslesced headers
  mChan.Send(mData);

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

    pStf.addStfData({ std::move(lHdrMsg), std::move(lDataMsg) });
  }
}

std::unique_ptr<SubTimeFrame> CoalescedHdrDataDeserializer::deserialize(FairMQChannel& pChan)
{
  mHdrs.clear();
  mData.clear();

  const std::int64_t ret = pChan.Receive(mData, 500 /* ms */);

  // timeout ?
  if (ret == -2) {
    mData.clear();
    return nullptr;
  }

  if (ret < 0) {
    DDLOGF_GRL(1000, DataDistSeverity::error, "STF receive failed err={} errno={} error={}", ret, errno,
      std::string(strerror(errno)));

    mData.clear();
    return nullptr;
  }

  return deserialize_impl();
}

std::unique_ptr<SubTimeFrame> CoalescedHdrDataDeserializer::deserialize(FairMQParts& pMsgs)
{
  mHdrs.clear();
  mData.clear();

  swap(mData, pMsgs.fParts);
  pMsgs.fParts.clear();

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

      auto lNewHdr = mTfBld.newHeaderMessage(lHdrInfo.len);
      std::memcpy(lNewHdr->GetData(), lFullHdrMsgAddr + lHdrOff, lHdrInfo.len);

      lInfoOff += sizeof(CoalescedHdrDataSerializer::header_info);
      lHdrOff += lHdrInfo.len;

      mHdrs.emplace_back(std::move(lNewHdr));
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

}
} /* o2::DataDistribution */
