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

void IovSerializer::visit(SubTimeFrame& pStf)
{
  // prepare headers
  mIovHeader.set_stf_id(pStf.id());
  mIovHeader.set_stf_size(pStf.getDataSize());

  // Pack the Stf header
  mIovHeader.set_stf_dd_header(&pStf.header(), sizeof(SubTimeFrame::Header));

  std::uint64_t lDataIovIdx = 0;

  for (auto& lDataIdentMapIter : pStf.mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {

        if (lStfDataIter.mHeader) {
          const DataHeader *lHdrPtr = reinterpret_cast<DataHeader*>(lStfDataIter.mHeader->GetData());

          // add the header
          auto lHdrMeta = mIovHeader.add_stf_hdr_iov();

          lHdrMeta->set_hdr_data(lStfDataIter.mHeader->GetData(), lStfDataIter.mHeader->GetSize());
          lHdrMeta->set_num_data_parts(lHdrPtr->splitPayloadParts > 1 ? lHdrPtr->splitPayloadParts : 1);
        }

        // add the data
        for (auto &lDataMsg : lStfDataIter.mDataParts) {
          (void) lDataIovIdx;
#if 0 // Not needed for FairMQ channels
          auto lDataMeta = mIovHeader.add_stf_data_iov();
          lDataMeta->set_iov_idx(lDataIovIdx++);
          lDataMeta->set_iov_start(0);
          lDataMeta->set_iov_size(lDataMsg->GetSize());
#endif
          mData.push_back(std::move(lDataMsg));
        }
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
void IovDeserializer::visit(SubTimeFrame& pStf)
{
  // stf header
  const auto lHdrSize = std::min(mIovHeader.stf_dd_header().size(), sizeof(SubTimeFrame::Header));
  memcpy(&pStf.mHeader, mIovHeader.stf_dd_header().c_str(), lHdrSize);

  std::size_t iData = 0;
  for (std::size_t iHdr = 0; iHdr < mHdrs.size(); iHdr++) {

    auto lHdrMsg = std::move(mHdrs[iHdr]);
    const auto lLastDataIdx = mIovHeader.stf_hdr_iov(iHdr).num_data_parts() + iData;

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

std::unique_ptr<SubTimeFrame> IovDeserializer::deserialize(FairMQChannel& pChan, bool pLogError)
{
  FairMQMessagePtr lHdrMetaMsg;
  mData.clear();

  const std::int64_t lHdrRet = pChan.Receive(lHdrMetaMsg, 500 /* ms */);

  switch (lHdrRet) {
    case static_cast<std::int64_t>(fair::mq::TransferCode::timeout):
      return nullptr;
      break;
    case static_cast<std::int64_t>(fair::mq::TransferCode::interrupted):
      if (pLogError) {
        IDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::interrupted");
      }
      return nullptr;
      break;
    case static_cast<std::int64_t>(fair::mq::TransferCode::error):
      EDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::error err={} errno={} error={}",
        int(lHdrRet), errno, std::string(strerror(errno)));
      mData.clear();
      return nullptr;
      break;
    default: // data or zero
      if (lHdrRet < 0) {
        WDDLOG_RL(1000, "STF receive failed. what=zero_size");
        return nullptr;
      }
      break;
  }

  // unpack headers
  mIovHeader.ParseFromArray(lHdrMetaMsg->GetData(), lHdrMetaMsg->GetSize());

  // receive data
  while (true) {
    mData.clear();
    const std::int64_t lDataRet = pChan.Receive(mData, 500 /* ms */);

    switch (lDataRet) {
      case static_cast<std::int64_t>(fair::mq::TransferCode::timeout):
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
          int(lDataRet), errno, std::string(strerror(errno)));
        mData.clear();
        return nullptr;
        break;
      default: // data or zero
        if (lDataRet > 0) {
          return deserialize_impl();
        } else {
          WDDLOG_RL(1000, "STF receive failed. what=zero_size");
          mData.clear();
          return nullptr;
        }
        break;
    }
  }

  assert (false);
  return nullptr;
}

std::unique_ptr<SubTimeFrame> IovDeserializer::deserialize(FairMQMessagePtr &pHdrMetaMsg, std::vector<FairMQMessagePtr>& pDataMsgs)
{
  // unpack headers
  mIovHeader.Clear();
  mIovHeader.ParseFromArray(pHdrMetaMsg->GetData(), pHdrMetaMsg->GetSize());

  mData = std::move(pDataMsgs);
  pDataMsgs.clear();

  return deserialize_impl();
}


std::unique_ptr<SubTimeFrame> IovDeserializer::deserialize_impl()
{
  std::unique_ptr<SubTimeFrame> lStf = nullptr;
  try {
    // recreate header messages
    for (auto &lHdr : mIovHeader.stf_hdr_iov()) {
      auto lNewHdr = mTfBld.newHeaderMessage(lHdr.hdr_data().c_str(), lHdr.hdr_data().size());
      mHdrs.push_back(std::move(lNewHdr));
    }

    lStf = std::make_unique<SubTimeFrame>(mIovHeader.stf_id());
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


SubTimeFrame::Header IovDeserializer::peek_tf_header(FairMQMessagePtr& pHeaderMsg) const
{
  // unpack headers
  IovStfHeader lIovHeader;
  lIovHeader.ParseFromArray(pHeaderMsg->GetData(), pHeaderMsg->GetSize());

  SubTimeFrame::Header lStfHdr;
  auto lSize = std::min(lIovHeader.stf_dd_header().size(), sizeof(SubTimeFrame::Header ));
  memcpy(&lStfHdr, lIovHeader.stf_dd_header().c_str(), lSize);

  return lStfHdr;
}

// copy all messages into the data region, and update the vector
bool IovDeserializer::copy_to_region(std::vector<FairMQMessagePtr>& pMsgs /* in/out */)
{
  mTfBld.newDataMessages(pMsgs, pMsgs);

  return true;
}


} /* o2::DataDistribution */
