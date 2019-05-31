// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "SubTimeFrameDPL.h"
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
      LOG(ERROR) << "Allocation error: Stf DataHeader::size: " << sizeof(DataHeader);
      throw std::bad_alloc();
    }

    std::memcpy(lDataHeaderMsg->GetData(), lHdrStack.data(), lHdrStack.size());

    auto lDataMsg = mChan.NewMessage(sizeof(SubTimeFrame::Header));
    if (!lDataMsg) {
      LOG(ERROR) << "Allocation error: Stf::Header::size: " << sizeof(SubTimeFrame::Header);
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

  mChan.Send(mMessages);

  // make sure headers and chunk pointers don't linger
  mMessages.clear();
  pStf.reset();
}
}
} /* o2::DataDistribution */
