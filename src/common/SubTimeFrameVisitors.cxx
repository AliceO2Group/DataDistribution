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
    DDLOG(fair::Severity::ERROR) << "Allocation error: Stf DataHeader::size: " << sizeof(DataHeader);
    throw std::bad_alloc();
  }
  std::memcpy(lDataHeaderMsg->GetData(), &gStfDistDataHeader, sizeof(DataHeader));

  auto lDataMsg = mChan.NewMessage(sizeof(SubTimeFrame::Header));
  if (!lDataMsg) {
    DDLOG(fair::Severity::ERROR) << "Allocation error: Stf::Header::size: " << sizeof(SubTimeFrame::Header);
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
          DDLOG(fair::Severity::ERROR) << "Sending STF data payload with zero size";
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
   DDLOG(fair::Severity::WARNING) << "Receiving bad SubTimeFrame::Header::DataHeader message";
    throw std::runtime_error("SubTimeFrame::Header::DataHeader");
  }
  // copy the header
  std::memcpy(&pStf.mHeader, mMessages[1]->GetData(), sizeof(SubTimeFrame::Header));

  // iterate over all incoming HBFrame data sources
  for (size_t i = 2; i < mMessages.size(); i += 2) {

    auto &lDataMsg = mMessages[i + 1];

    if (lDataMsg->GetSize() == 0) {
      DDLOG(fair::Severity::ERROR) << "Received STF data payload with zero size";
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
    DDLOG(fair::Severity::ERROR) << "SubTimeFrame deserialization failed. Reason: " << e.what();
    mMessages.clear();
    return nullptr; // TODO: what? FMQ.Receive() does not throw...?
  } catch (std::exception& e) {
    DDLOG(fair::Severity::ERROR) << "SubTimeFrame deserialization failed. Reason: " << e.what();
    mMessages.clear();
    return nullptr;
  }

  // make sure headers and chunk pointers don't linger
  mMessages.clear();

  return lStf;
}

}
} /* o2::DataDistribution */
