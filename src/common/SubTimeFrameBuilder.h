// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

#ifndef ALICEO2_SUBTIMEFRAME_BUILDER_H_
#define ALICEO2_SUBTIMEFRAME_BUILDER_H_

#include "SubTimeFrameDataModel.h"
#include "MemoryUtils.h"

#include <Headers/DataHeader.h>
#include <Headers/Stack.h>
#include <Framework/DataProcessingHeader.h>

#include <vector>
#include <mutex>
#include <optional>

#include <fairmq/FwdDecls.h>

namespace o2::DataDistribution
{

////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameReadoutBuilder
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameReadoutBuilder
{
 public:
  SubTimeFrameReadoutBuilder() = delete;
  SubTimeFrameReadoutBuilder(SyncMemoryResources &pMemRes);

  bool addHbFrames(const o2::header::DataOrigin &pDataOrig,
    const o2::header::DataHeader::SubSpecificationType pSubSpecification,
    const ReadoutSubTimeframeHeader& pHdr,
    std::vector<fair::mq::MessagePtr>::iterator pHbFramesBegin, const std::size_t pHBFrameLen);

  std::optional<std::unique_ptr<SubTimeFrame>> addTopoStfData(const o2::header::DataOrigin &pDataOrig,
    const o2::header::DataHeader::SubSpecificationType pSubSpecification,
    const ReadoutSubTimeframeHeader& pHdr,
    std::vector<fair::mq::MessagePtr>::iterator &pHbFramesBegin, std::size_t &pHBFrameLen,
    const std::uint64_t pMaxNumMessages, bool pCutTfOnNewOrbit);

  std::optional<std::uint32_t> getCurrentStfId() const {
    return (mStf) ? std::optional<std::uint32_t>(mStf->header().mId) : std::nullopt;
  }

  std::optional<std::unique_ptr<SubTimeFrame>> getStf() {

    std::unique_ptr<SubTimeFrame> lStf = std::move(mStf);

    auto lRet = (lStf && mAcceptStfData) ? std::optional<std::unique_ptr<SubTimeFrame>>(std::move(lStf)) : std::nullopt;

    mStf = nullptr;
    mAcceptStfData = true;
    mFirstFiltered.clear();

    return lRet;
  }

  // for aggregation of threshold scan data
  std::optional<std::unique_ptr<SubTimeFrame>> getTopoStf() {
    if (mTopoStfMap.empty()) {
      return std::nullopt;
    }

    const auto lBegin = mTopoStfMap.begin();
    std::unique_ptr<SubTimeFrame> lStf = std::move(lBegin->second.second);
    mTopoStfMap.erase(lBegin);

    if (lStf) {
      lStf->setOrigin(SubTimeFrame::Header::Origin::eReadoutTopology);
    }

    mAcceptStfData = true;
    return (lStf) ? std::optional<std::unique_ptr<SubTimeFrame>>(std::move(lStf)) : std::nullopt;
  }

  inline void stop() {
    mRunning = false;
    mMemRes.stop();
  }

 private:
  bool mRunning = true;

  std::unique_ptr<SubTimeFrame> mStf;

  // Build Stfs for topological scans
  std::unordered_map<header::DataHeader::SubSpecificationType, std::pair<std::uint64_t, std::unique_ptr<SubTimeFrame>> > mTopoStfMap;

  bool mAcceptStfData = true;        // toggle on allocation issues

  // filtering: keep info if the first HBFrame is already kept back
  std::unordered_map<o2::header::DataHeader::SubSpecificationType, bool> mFirstFiltered;

  SyncMemoryResources &mMemRes;
};


////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameFileBuilder
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameFileBuilder
{
 public:
  SubTimeFrameFileBuilder() = delete;
  SubTimeFrameFileBuilder(SyncMemoryResources &pMemRes,
    const std::size_t pDataSegSize, const std::optional<std::uint16_t> pDataSegId,
    const std::size_t pHdrSegSize, const std::optional<std::uint16_t> pHdrSegId);

  void adaptHeaders(SubTimeFrame *pStf);

  // allocate appropriate message for the header
  inline
  fair::mq::MessagePtr newHeaderMessage(const o2::header::Stack &pIncomingStack, const std::uint64_t pTfId) {
    auto lStack = o2::header::Stack(
      pIncomingStack,
      o2::framework::DataProcessingHeader{pTfId}
    );

    return mMemRes.newHeaderMessage(lStack.data(), lStack.size());
  }

  // allocate appropriate message for the data blocks
  inline
  fair::mq::MessagePtr newDataMessage(const std::size_t pSize) {
    return mMemRes.newDataMessage(pSize);
  }

  void stop() {
    mMemRes.stop();
  }

 private:
  SyncMemoryResources &mMemRes;
};

////////////////////////////////////////////////////////////////////////////////
/// TimeFrameBuilder
////////////////////////////////////////////////////////////////////////////////

class TimeFrameBuilder
{
 public:
  TimeFrameBuilder() = delete;
  TimeFrameBuilder(SyncMemoryResources &pMemRes);

  // allocate the memory here
  void allocate_memory(const std::size_t pDataSegSize, const std::optional<std::uint16_t> pDataSegId,
                       const std::size_t pHdrSegSize, const std::optional<std::uint16_t> pHdrSegId);

  void adaptHeaders(SubTimeFrame *pStf);


  fair::mq::MessagePtr newHeaderMessage(const char *pData, const std::size_t pSize);

  inline
  fair::mq::MessagePtr newDataMessage(const std::size_t pSize) {
    return mMemRes.newDataMessage(pSize);
  }

  inline
  fair::mq::MessagePtr newDataMessage(const char *pData, const std::size_t pSize) {
    return mMemRes.newDataMessage(pData, pSize);
  }

  inline
  bool replaceDataMessages(std::vector<fair::mq::MessagePtr> &pMsgs) {
    return mMemRes.replaceDataMessages(pMsgs);
  }

  inline void stop() {
    mMemRes.stop();
  }

  inline auto freeData() const { return mMemRes.freeData(); }

  // support for ucx txg allocations
  inline void allocDataBuffers(const std::vector<uint64_t> &pTxgSizes, std::vector<void*> &pTxgPtrs) {
    mMemRes.allocDataBuffers(pTxgSizes, std::back_inserter(pTxgPtrs));
  }

  inline void allocHeaderMsgs(const std::vector<uint64_t> &pTxgSizes, std::vector<fair::mq::MessagePtr> &pHdrVec) {
    mMemRes.allocHdrBuffers(pTxgSizes, std::back_inserter(pHdrVec));
  }

  inline void newDataFmqMessagesFromPtr(const std::vector<std::pair<void*, std::size_t>> &pPtrSizes, std::vector<fair::mq::MessagePtr> &pDataVec) {
    mMemRes.fmqFromDataBuffers(pPtrSizes, std::back_inserter(pDataVec));
  }

  SyncMemoryResources &mMemRes;
};


////////////////////////////////////////////////////////////////////////////////
/// SubTimeFrameCopyBuilder
////////////////////////////////////////////////////////////////////////////////

class SubTimeFrameCopyBuilder
{
 public:
  SubTimeFrameCopyBuilder() = delete;
  SubTimeFrameCopyBuilder(SyncMemoryResources &pMemRes)
  : mMemRes(pMemRes)
  { }

  // make allocate the memory here
  void allocate_memory(const std::size_t pDataSegSize, const std::optional<std::uint16_t> pDataSegId);

  void allocNewStfData(const std::unique_ptr<SubTimeFrame> &pStf, std::vector<void*> &pLinkBuffers);
  bool copyStfData(std::unique_ptr<SubTimeFrame> &pStf, const std::vector<void*> &pLinkBuffers);

  inline auto freeData() const { return mMemRes.freeData(); }

  inline void stop() {
    mMemRes.stop();
  }

  SyncMemoryResources &mMemRes;
};



} /* o2::DataDistribution */

#endif /* ALICEO2_SUBTIMEFRAME_BUILDER_H_ */
