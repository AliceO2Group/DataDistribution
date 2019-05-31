// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "SubTimeFrameBuilder.h"
#include "MemoryUtils.h"

#include <Headers/DataHeader.h>
#include <Headers/Stack.h>
#include <Framework/DataProcessingHeader.h>

#include <fairmq/FairMQDevice.h>
#include <fairmq/FairMQUnmanagedRegion.h>

namespace o2
{
namespace DataDistribution
{

using namespace o2::header;

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameReadoutBuilder
////////////////////////////////////////////////////////////////////////////////

SubTimeFrameReadoutBuilder::SubTimeFrameReadoutBuilder(FairMQDevice &pDev, FairMQChannel& pChan, bool pDplEnabled)
  : mStf(nullptr),
    mDevice(pDev),
    mChan(pChan),
    mDplEnabled(pDplEnabled)
{
  mHeaderMemRes = std::make_unique<FMQUnsynchronizedPoolMemoryResource>(
    pDev, pChan,
    64ULL << 20 /* make configurable */,
    mDplEnabled ?
      sizeof(DataHeader) + sizeof(o2::framework::DataProcessingHeader) :
      sizeof(DataHeader)
  );
}

void SubTimeFrameReadoutBuilder::addHbFrames(const ReadoutSubTimeframeHeader& pHdr, std::vector<FairMQMessagePtr>&& pHbFrames)
{
  if (!mStf) {
    mStf = std::make_unique<SubTimeFrame>(pHdr.timeframeId);
  }

  assert(pHdr.timeframeId == mStf->header().mId);

  const EquipmentIdentifier lEqId = EquipmentIdentifier(
    o2::header::gDataDescriptionRawData,
    o2::header::gDataOriginFLP, // FIXME: proper equipment specification
    pHdr.linkId);

  // NOTE: skip the first message (readout header) in pHbFrames
  for (size_t i = 1; i < pHbFrames.size(); i++) {

    std::unique_ptr<FairMQMessage> lHdrMsg;

    DataHeader lDataHdr(
      lEqId.mDataDescription,
      lEqId.mDataOrigin,
      lEqId.mSubSpecification,
      pHbFrames[i]->GetSize());
    lDataHdr.payloadSerializationMethod = gSerializationMethodNone;

    if (mDplEnabled) {
      o2::framework::DataProcessingHeader lDplHeader(mStf->header().mId);

      // Is there another way to compose headers? Stack is heavy on malloc/free needlessly
      // auto lStack = Stack(Stack::allocator_type(mHeaderMemRes.get()), lDataHdr, lDplHeader);
      auto lStack = Stack(lDataHdr, lDplHeader);
      lHdrMsg = mHeaderMemRes->NewFairMQMessage();
      if (!lHdrMsg ||
        (lHdrMsg->GetSize() < (sizeof(DataHeader) + sizeof(o2::framework::DataProcessingHeader)))) {
        throw std::bad_alloc();
      }

      memcpy(lHdrMsg->GetData(), lStack.data(), lHdrMsg->GetSize());

    } else {

      lHdrMsg = mHeaderMemRes->NewFairMQMessage();
      memcpy(lHdrMsg->GetData(), &lDataHdr, sizeof(DataHeader));
    }

    if (!lHdrMsg) {
      LOG(ERROR) << "Allocation error: HbFrame::DataHeader: " << sizeof(DataHeader);
      throw std::bad_alloc();
    }

    mStf->addStfData(lDataHdr,
      SubTimeFrame::StfData{ std::move(lHdrMsg), std::move(pHbFrames[i]) }
    );
  }

  pHbFrames.clear();
}

std::unique_ptr<SubTimeFrame> SubTimeFrameReadoutBuilder::getStf()
{
  return std::move(mStf);
}

}
} /* o2::DataDistribution */
