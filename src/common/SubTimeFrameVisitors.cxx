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
/// IovSerializer
////////////////////////////////////////////////////////////////////////////////

void IovSerializer::visit(SubTimeFrame& pStf, void*)
{
  // prepare headers
  mIovHeader.mutable_stf_hdr_meta()->set_stf_id(pStf.id());
  mIovHeader.mutable_stf_hdr_meta()->set_stf_size(pStf.getDataSize());

  // Pack the Stf header
  mIovHeader.mutable_stf_hdr_meta()->set_stf_dd_header(&pStf.header(), sizeof(SubTimeFrame::Header));

  for (auto& lDataIdentMapIter : pStf.mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {

        if (lStfDataIter.mHeader) {
          const DataHeader *lHdrPtr = reinterpret_cast<DataHeader*>(lStfDataIter.mHeader->GetData());

          // add the header
          auto lHdrMeta = mIovHeader.mutable_stf_hdr_meta()->add_stf_hdr_iov();

          lHdrMeta->set_hdr_data(lStfDataIter.mHeader->GetData(), lStfDataIter.mHeader->GetSize());
          lHdrMeta->set_num_data_parts(lHdrPtr->splitPayloadParts > 1 ? lHdrPtr->splitPayloadParts : 1);
        }

        // add the data
        std::move(lStfDataIter.mDataParts.begin(), lStfDataIter.mDataParts.end(), std::back_inserter(mData));
      }
    }
  }

  std::string lHeaderMessage;
  mIovHeader.SerializeToString(&lHeaderMessage);

  auto lHdrMetaMsg = mChan.NewMessage(lHeaderMessage.size());
  memcpy(lHdrMetaMsg->GetData(), lHeaderMessage.c_str(), lHeaderMessage.size());

  mData.push_back(std::move(lHdrMetaMsg));

  pStf.mData.clear();
  pStf.mHeader = SubTimeFrame::Header();
}

void IovSerializer::serialize(std::unique_ptr<SubTimeFrame>&& pStf)
{
  mData.clear();
  mIovHeader.Clear();

  pStf->accept(*this);

  // send the data + coslesced headers
  while (mRunning && (static_cast<std::int64_t>(fair::mq::TransferCode::timeout) == mChan.Send(mData, 500))) { }

  // make sure headers and chunk pointers don't linger
  mData.clear();
  mIovHeader.Clear();
}



////////////////////////////////////////////////////////////////////////////////
/// IovDeserializer
////////////////////////////////////////////////////////////////////////////////
void IovDeserializer::visit(SubTimeFrame& pStf, void*)
{
  // stf header
  const auto lHdrSize = std::min(mIovStfHeader.stf_dd_header().size(), sizeof(SubTimeFrame::Header));
  memcpy(&pStf.mHeader, mIovStfHeader.stf_dd_header().c_str(), lHdrSize);

  std::size_t iData = 0;
  for (std::size_t iHdr = 0; iHdr < mHdrs.size(); iHdr++) {

    auto lHdrMsg = std::move(mHdrs[iHdr]);
    const auto lLastDataIdx = mIovStfHeader.stf_hdr_iov(iHdr).num_data_parts() + iData;

    const DataHeader *lHdrPtr = reinterpret_cast<DataHeader*>(lHdrMsg->GetData());
    const auto lSubSpec = lHdrPtr->subSpecification;
    const o2hdr::DataIdentifier lDataId = impl::getDataIdentifier(*lHdrPtr);

    // add header message
    auto *lVec = pStf.addStfDataStart(lDataId, lSubSpec, {std::move(lHdrMsg), std::move(mData[iData])});

    for (iData += 1; iData < lLastDataIdx; iData++) {
      pStf.addStfDataAppend(lVec, std::move(mData[iData]));
    }
  }

  if (iData != mData.size()) {
    EDDLOG("BUG: data indexing does not match. hdr_cnt={} idx={} size={}", mHdrs.size(), iData, mData.size());
  }
}


std::unique_ptr<SubTimeFrame> IovDeserializer::deserialize(const IovStfHdrMeta &pHdrMeta, std::vector<fair::mq::MessagePtr>& pDataMsgs)
{
  mIovStfHeader.Clear();
  mIovStfHeader.CopyFrom(pHdrMeta);

  mData = std::move(pDataMsgs);
  pDataMsgs.clear();

  return deserialize_impl();
}


std::unique_ptr<SubTimeFrame> IovDeserializer::deserialize_impl()
{
  std::unique_ptr<SubTimeFrame> lStf = nullptr;
  try {
    // recreate header messages
    for (const auto &lHdr : mIovStfHeader.stf_hdr_iov()) {
      auto lNewHdr = mTfBld.newHeaderMessage(lHdr.hdr_data().c_str(), lHdr.hdr_data().size());
      mHdrs.push_back(std::move(lNewHdr));
    }

    lStf = std::make_unique<SubTimeFrame>(mIovStfHeader.stf_id());
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


SubTimeFrame::Header IovDeserializer::peek_tf_header(const IovStfHdrMeta &pHdrMeta) const
{
  SubTimeFrame::Header lStfHdr;
  auto lSize = std::min(pHdrMeta.stf_dd_header().size(), sizeof(SubTimeFrame::Header));
  memcpy(&lStfHdr, pHdrMeta.stf_dd_header().data(), lSize);

  return lStfHdr;
}

// copy all messages into the data region, and update the vector
bool IovDeserializer::copy_to_region(std::vector<fair::mq::MessagePtr>& pMsgs /* in/out */)
{
  mTfBld.newDataMessages(pMsgs, pMsgs);

  return true;
}


} /* o2::DataDistribution */
