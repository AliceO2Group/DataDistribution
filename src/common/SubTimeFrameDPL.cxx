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

void StfDplAdapter::visit(SubTimeFrame& pStf)
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

void StfDplAdapter::sendToDpl(std::unique_ptr<SubTimeFrame>&& pStf)
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
}
} /* o2::DataDistribution */
