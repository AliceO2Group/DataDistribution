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

#include <fairmq/FairMQDevice.h>
#include <fairmq/FairMQUnmanagedRegion.h>

namespace o2
{
namespace DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameReadoutBuilder
////////////////////////////////////////////////////////////////////////////////

SubTimeFrameReadoutBuilder::SubTimeFrameReadoutBuilder(FairMQDevice &pDev, FairMQChannel& pChan)
  : mStf(nullptr),
    mDevice(pDev),
    mChan(pChan)
{
  mHeaderRegion = pDev.NewUnmanagedRegionFor(
    pChan.GetPrefix(), 0,
    64ULL << 20,
      [this](void* data, size_t size, void* /* hint */) {
      // callback to be called when message buffers no longer needed by transport
      reclaimHeader(static_cast<DataHeader*>(data), size);
    });

  // prepare header pointers
  DataHeader* lDataHdr = static_cast<DataHeader*>(mHeaderRegion->GetData());
  const std::size_t lDataHdrCnt = mHeaderRegion->GetSize() / sizeof(DataHeader);
  for (std::size_t i = 0; i < lDataHdrCnt; i++) {
    memset(lDataHdr+i, 0xAA, sizeof(DataHeader));
    mHeaders.push_back(lDataHdr + i);
  }

  LOG(INFO) << "Header memory pool created.";
}

FairMQMessagePtr SubTimeFrameReadoutBuilder::allocateHeader()
{
  {
    std::lock_guard<std::mutex> lock(mHeaderLock);

    if (!mHeaders.empty()) {
      auto lHdrPtr = mHeaders.back();
      mHeaders.pop_back();

      return mChan.NewMessage(mHeaderRegion, lHdrPtr, sizeof(DataHeader));
    }
  }

  {
    static thread_local unsigned throttle = 0;
    if (++throttle > (1U << 18)) {
      LOG(WARNING) << "Header pool exhausted. Allocating from the global SHM pool.";
      throttle = 0;
    }
  }

  return mChan.NewMessage(sizeof(DataHeader));
}

void SubTimeFrameReadoutBuilder::reclaimHeader(DataHeader* pData, size_t pSize)
{
  if (pSize != sizeof(DataHeader)) {
    LOG(ERROR) << "Reclaimed header has invalid size: " << pSize;
    return;
  }

  mReclaimedHeaders.push_back(pData);

  // reclaim
  if (mReclaimedHeaders.size() >= 512) {
    std::lock_guard<std::mutex> lock(mHeaderLock);

    mHeaders.insert(std::end(mHeaders), std::begin(mReclaimedHeaders), std::end(mReclaimedHeaders));
    mReclaimedHeaders.clear();
  }
}

void SubTimeFrameReadoutBuilder::visit(SubTimeFrame& /* pStf */)
{
  /* empty */
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
    DataHeader lDataHdr(
      lEqId.mDataDescription,
      lEqId.mDataOrigin,
      lEqId.mSubSpecification,
      pHbFrames[i]->GetSize());
    lDataHdr.payloadSerializationMethod = gSerializationMethodNone;

    auto lHdrMsg = allocateHeader();
    if (!lHdrMsg) {
      LOG(ERROR) << "Allocation error: HbFrame::DataHeader: " << sizeof(DataHeader);
      throw std::bad_alloc();
    }
    std::memcpy(lHdrMsg->GetData(), &lDataHdr, sizeof(DataHeader));

    mStf->addStfData(lDataHdr, SubTimeFrame::StfData{ std::move(lHdrMsg), std::move(pHbFrames[i]) });
  }

  pHbFrames.clear();
}

std::unique_ptr<SubTimeFrame> SubTimeFrameReadoutBuilder::getStf()
{
  return std::move(mStf);
}

}
} /* o2::DataDistribution */
