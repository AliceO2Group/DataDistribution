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

#include "StfSenderOutputUCX.h"

#include <DataDistributionOptions.h>
#include <DataDistLogger.h>
#include <DataDistMonitoring.h>

#include <UCXSendRecv.h>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

static void client_ep_err_cb(void *arg, ucp_ep_h ep, ucs_status_t status)
{
    (void) ep;
    StfSenderUCXConnInfo *lConnInfo = reinterpret_cast<StfSenderUCXConnInfo*>(arg);
    lConnInfo->mOutputUCX->handle_client_ep_error(lConnInfo, status);
}

StfSenderOutputUCX::StfSenderOutputUCX(std::shared_ptr<ConsulStfSender> pDiscoveryConfig, StdSenderOutputCounters &pCounters)
  : mDiscoveryConfig(pDiscoveryConfig),
    mCounters(pCounters)
{
}

bool StfSenderOutputUCX::start()
{
  // setting configuration options
  mRmaGap = std::clamp(mDiscoveryConfig->getUInt64Param(UcxRdmaGapBKey, UcxRdmaGapBDefault), std::size_t(0), std::size_t(16 << 20));
  mThreadPoolSize = std::clamp(mDiscoveryConfig->getUInt64Param(UcxSenderThreadPoolSizeKey, UcxStfSenderThreadPoolSizeDefault), std::size_t(0), std::size_t(128));
  mThreadPoolSize = std::max(std::size_t(8), (mThreadPoolSize == 0) ? std::thread::hardware_concurrency() : mThreadPoolSize);

  IDDLOG("StfSenderOutputUCX: Configuration loaded. rma_gap={} thread_pool={}", mRmaGap, mThreadPoolSize);

  // Create the UCX context
  if (!ucx::util::create_ucp_context(&ucp_context)) {
    EDDLOG("StfSenderOutputUCX: failed to create UCX context");
    return false;
  }

  // start thread pool
  for (std::size_t i = 0; i < mThreadPoolSize; i++) {
    std::string lThreadName = "stfs_ucx_" + std::to_string(i);

    mThreadPool.emplace_back(
      std::move(create_thread_member(lThreadName.c_str(), &StfSenderOutputUCX::DataHandlerThread, this, i))
    );
  }

  // start dealloc thread
  mDeallocThread = std::move(create_thread_member("stfs_dealloc", &StfSenderOutputUCX::StfDeallocThread, this));

  // we can accept connections
  mRunning = true;
  return true;
}

void StfSenderOutputUCX::stop()
{
  mRunning = false;

  // stop the thread pool
  mSendRequestQueue.stop();
  mStfDeleteQueue.stop();
  for (auto &lThread : mThreadPool) {
    if (lThread.joinable()) {
      lThread.join();
    }
  }
  mThreadPool.clear();

  if (mDeallocThread.joinable()) {
    mDeallocThread.join();
  }
  DDDLOG("StfSenderOutputUCX::stop: stopped all threads.");

  // Revoke all rkeys and mappings
  {
    std::scoped_lock lLock(mRegionListLock);
    for (auto &lMapping : mRegions) {
      ucx::util::destroy_rkey_for_region(ucp_context, lMapping.ucp_mem, lMapping.ucp_rkey_buf);
    }
  }
  DDDLOG("StfSenderOutputUCX::stop: revoked all rkeys.");

  // close all connections
  {
    std::scoped_lock lLock(mOutputMapLock);
    // for (auto &lConn : mOutputMap) {
    //   ucx::util::close_connection(lConn.second->worker, lConn.second->ucp_ep);
    // }
    mOutputMap.clear();
  }
  DDDLOG("StfSenderOutputUCX::stop: closed all connections.");
}

ConnectStatus StfSenderOutputUCX::connectTfBuilder(const std::string &pTfBuilderId, const std::string &lTfBuilderIp, const unsigned lTfBuilderPort)
{
  if (!mRunning.load()) {
    EDDLOG_ONCE("StfSenderOutputUCX::connectTfBuilder: backend is not started.");
    return eCONNERR;
  }

  // Check if connection already exists
  {
    std::scoped_lock lLock(mOutputMapLock);

    if (mOutputMap.count(pTfBuilderId) > 0) {
      EDDLOG("StfSenderOutputUCX::connectTfBuilder: TfBuilder is already connected. tfb_id={}", pTfBuilderId);
      return eEXISTS;
    }
  }

  auto lConnInfo = std::make_unique<StfSenderUCXConnInfo>(this, pTfBuilderId);

  // create worker the TfBuilder connection
  if (!ucx::util::create_ucp_worker(ucp_context, &lConnInfo->worker, pTfBuilderId)) {
    return eCONNERR;
  }

  // create endpoint for TfBuilder connection
  DDDLOG("Connect to TfBuilder ip={} port={}", lTfBuilderIp, lTfBuilderPort);
  if (!ucx::util::create_ucp_client_ep(lConnInfo->worker.ucp_worker, lTfBuilderIp, lTfBuilderPort,
    &lConnInfo->ucp_ep, client_ep_err_cb, lConnInfo.get(), pTfBuilderId)) {

    return eCONNERR;
  }
  const std::string lStfSenderId = mDiscoveryConfig->status().info().process_id();
  auto lOk = ucx::io::ucx_send_string(lConnInfo->worker, lConnInfo->ucp_ep, lStfSenderId);
  if (!lOk) {
    EDDLOG("connectTfBuilder: Sending of local id failed.");
    return eCONNERR;
  }

  // Add the connection to connection map
  {
    std::scoped_lock lLock(mOutputMapLock);
    const auto lItOk = mOutputMap.try_emplace(pTfBuilderId, std::move(lConnInfo));
    if (!lItOk.second) {
      EDDLOG("connectTfBuilder: TfBuilder connection already exists tfbuilder_id={}", pTfBuilderId);
      return eCONNERR;
    }
  }
  DDDLOG("StfSenderOutputUCX::connectTfBuilder: transport started for tfbuilder_id={}", pTfBuilderId);
  return eOK;
}

bool StfSenderOutputUCX::disconnectTfBuilder(const std::string &pTfBuilderId)
{
  // find and remove from the connection map
  std::unique_ptr<StfSenderUCXConnInfo> lConnInfo;
  {
    std::scoped_lock lLock(mOutputMapLock);

    auto lIt = mOutputMap.find(pTfBuilderId);

    if (lIt == mOutputMap.end()) {
      DDDLOG("StfSenderOutputUCX::disconnectTfBuilder: TfBuilder was not connected. tfb_id={}", pTfBuilderId);
      return true;
    }
    lConnInfo = std::move(mOutputMap.extract(lIt).mapped());
  }

  // Transport is only closed when other side execute close as well. Execute async
  std::thread([pConnInfo = std::move(lConnInfo), pTfBuilderId](){
    DDDLOG("StfSenderOutputUCX::disconnectTfBuilder: closing transport for tf_builder={}", pTfBuilderId);
    // acquire the lock and close the connection
    std::unique_lock lTfSenderLock(pConnInfo->mTfBuilderLock);
    ucx::util::close_connection(pConnInfo->worker, pConnInfo->ucp_ep);
    DDDLOG("StfSenderOutputUCX::disconnectTfBuilder: transport stopped for tf_builder={}", pTfBuilderId);
  }).detach();

  return true;
}

bool StfSenderOutputUCX::sendStfToTfBuilder(const std::string &pTfBuilderId, std::unique_ptr<SubTimeFrame> &&pStf)
{
  mSendRequestQueue.push(SendStfInfo{std::move(pStf), pTfBuilderId});
  return true;
}

void StfSenderOutputUCX::visit(const SubTimeFrame &pStf, void *pData)
{
  using UCXData = UCXIovStfHeader::UCXData;
  using UCXIovTxg = UCXIovStfHeader::UCXIovTxg;
  using UCXRegion = UCXIovStfHeader::UCXRegion;

  const auto lStfSize = pStf.getDataSize();

  UCXIovStfHeader *lStfUCXMeta = reinterpret_cast<UCXIovStfHeader*>(pData);

  // set basic STF info
  lStfUCXMeta->mutable_stf_hdr_meta()->set_stf_id(pStf.id());
  lStfUCXMeta->mutable_stf_hdr_meta()->set_stf_size(pStf.getDataSize());
  // Pack the Stf header
  lStfUCXMeta->mutable_stf_hdr_meta()->set_stf_dd_header(&pStf.header(), sizeof(SubTimeFrame::Header));

  // pack all headers and collect data
  std::vector<UCXData> lStfDataPtrs;
  lStfDataPtrs.reserve(32768);

  std::uint64_t lDataIovIdx = 0;

  for (auto& lDataIdentMapIter : pStf.mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {
        if (lStfDataIter.mHeader) {
          assert (lStfDataIter.mDataParts.size() > 0);
          // add the header
          auto lHdrMeta = lStfUCXMeta->mutable_stf_hdr_meta()->add_stf_hdr_iov();
          lHdrMeta->set_hdr_data(lStfDataIter.mHeader->GetData(), lStfDataIter.mHeader->GetSize());
          lHdrMeta->set_num_data_parts(lStfDataIter.mDataParts.size());

          // add the data part info
          for (auto &lDataMsg : lStfDataIter.mDataParts) {
            lStfDataPtrs.emplace_back(UCXData());
            lStfDataPtrs.back().set_idx(lDataIovIdx++);
            lStfDataPtrs.back().set_len(lDataMsg->GetSize());
            // add pointer for start, then reset to region offset later
            lStfDataPtrs.back().set_start(reinterpret_cast<std::uint64_t>(lDataMsg->GetData()));
          }
        }
      }
    }
  }

  std::size_t lTotalGap = 0;

  if (!lStfDataPtrs.empty()) {
    // sort all data parts by pointer value in order to make txgs
    std::sort(lStfDataPtrs.begin(), lStfDataPtrs.end(), [](const UCXData &a, const UCXData &b) { return a.start() < b.start(); } );

    // create transactions
    std::uint32_t lTxgIdx = 0;
    std::uint32_t lRegionIdx = 0;

    UCXIovTxg *lStfTxg = lStfUCXMeta->add_stf_txg_iov();

    UCXData *lData = &lStfDataPtrs.front();

    const UCXMemoryRegionInfo* lRunningRegion = regionLookup(lData->start(), lData->len());
    // create the first region
    UCXRegion *lStfRegion = lStfUCXMeta->add_data_regions();
    lStfRegion->set_region(lRegionIdx++);
    lStfRegion->set_size(lRunningRegion->mSize);
    lStfRegion->set_region_rkey(lRunningRegion->ucp_rkey_buf, lRunningRegion->ucp_rkey_buf_size);

    // Update the start to region offset
    lData->set_txg(lTxgIdx);

    // create the first txg
    lStfTxg->set_txg(lTxgIdx++);
    lStfTxg->set_start(lData->start());
    lStfTxg->set_len(lData->len());
    lStfTxg->set_data_parts(1);
    lStfTxg->set_region(lStfRegion->region());

    for (std::size_t i = 1; i < lStfDataPtrs.size(); i++) {
      lData = &lStfDataPtrs[i];

      const auto lReg = regionLookup(lData->start(), lData->len());

      if (*lReg == *lRunningRegion) {
        // check if we extend the current txg
        const auto lGap = lData->start() - (lStfTxg->start() + lStfTxg->len());
        if (lGap <= mRmaGap) {
          // extend the existing txg
          lStfTxg->set_len(lStfTxg->len() + lGap + lData->len());
          lStfTxg->set_data_parts(lStfTxg->data_parts() + 1);
          lTotalGap += lGap;
          // set the data part txg
          lData->set_txg(lStfTxg->txg());

          if (lGap > 0) {
            DDMON("stfsender", "ucx.rma_gap", lGap);
          }
          continue;
        }
      } else {
        // this is a different region. That means we need to add it into the region vector and
        // start a new txg for the current data buffer
        lRunningRegion = lReg;
        // add new region
        lStfRegion = lStfUCXMeta->add_data_regions();
        lStfRegion->set_region(lRegionIdx++);
        lStfRegion->set_size(lRunningRegion->mSize);
        lStfRegion->set_region_rkey(lRunningRegion->ucp_rkey_buf, lRunningRegion->ucp_rkey_buf_size);
      }

      // start a new txg
      lStfTxg = lStfUCXMeta->add_stf_txg_iov();
      lStfTxg->set_txg(lTxgIdx++);
      lStfTxg->set_start(lData->start());
      lStfTxg->set_len(lData->len());
      lStfTxg->set_data_parts(1);
      lStfTxg->set_region(lStfRegion->region());
      // set the data part txg
      lData->set_txg(lStfTxg->txg());
    }

    // prepare the message
    for (const auto & lDataPart: lStfDataPtrs) {
      auto lNewDataPtr = lStfUCXMeta->add_stf_data_iov();
      *lNewDataPtr = lDataPart;
    }

    DDMON("stfsender", "ucx.rma_gap_total", lTotalGap);
    if (lStfSize > 0) {
      DDMON("stfsender", "ucx.rma_gap_overhead", (double(lTotalGap) / double(lStfSize) * 100.));
    }

    DDDLOG_GRL(10000, "UCX pack total data_size={} data_cnt={} txg_size={} txg_cnt={} gap_size={}",
      pStf.getDataSize(), lStfUCXMeta->stf_data_iov_size(), pStf.getDataSize()+lTotalGap, lStfUCXMeta->stf_txg_iov_size(), lTotalGap);
  }

}

void StfSenderOutputUCX::prepareStfMetaHeader(const SubTimeFrame &pStf, UCXIovStfHeader *pStfUCXMeta)
{
  pStf.accept(*this, pStfUCXMeta);
}

/// Sending thread
void StfSenderOutputUCX::DataHandlerThread(unsigned pThreadIdx)
{
  DDDLOG("StfSenderOutputUCX: Starting the thread pool {}", pThreadIdx);

  const std::string lStfSenderId = mDiscoveryConfig->status().info().process_id();

  assert(mSendRequestQueue.is_running());

  std::uint64_t lNumSentStfs = 0;

  std::optional<SendStfInfo> lSendReqOpt;

  while ((lSendReqOpt = mSendRequestQueue.pop()) != std::nullopt) {
    SendStfInfo &lSendReq = lSendReqOpt.value();
    const auto &lTfBuilderId = lSendReq.mTfBuilderId;
    auto &lStf = lSendReq.mStf;

    StfSenderUCXConnInfo *lConnInfo;
    {
      // get the thread data.
      std::scoped_lock lLock(mOutputMapLock);
      if (mOutputMap.count(lTfBuilderId) == 1) {
        lConnInfo = mOutputMap[lTfBuilderId].get();

        if (lConnInfo && lConnInfo->mConnError) {
          EDDLOG_GRL(1000, "StfSenderOutputUCX: Skipping sending data because peer connection issues. tfbuilder_id={}", lConnInfo->mTfBuilderId);
          mStfDeleteQueue.push(std::move(lStf));
          continue;
        }
      } else {
        mStfDeleteQueue.push(std::move(lStf));
        continue;
      }
    }

    const auto lStfId = lStf->id();
    const auto lStfSize = lStf->getDataSize();

    UCXIovStfHeader lMeta;
    prepareStfMetaHeader(*lStf, &lMeta);

    std::string lStfMetaData = lMeta.SerializeAsString();

    DDDLOG_GRL(5000, "Sending an STF to TfBuilder. stf_id={} tfb_id={} stf_size={} total_sent_stf={} meta_size={}",
      lStfId, lTfBuilderId, lStfSize, lNumSentStfs, lStfMetaData.size());

    // Send meta and wait for ack (locked)
    { // lock the TfBuilder for sending
      std::scoped_lock lTfBuilderLock(lConnInfo->mTfBuilderLock);

      if (ucx::io::ucx_send_string(lConnInfo->worker, lConnInfo->ucp_ep, lStfMetaData)) {
        // wait here until we get cometed notification
        auto lOkStrOpt = ucx::io::ucx_receive_string(lConnInfo->worker);
        if (!(lOkStrOpt.has_value() && (lOkStrOpt.value() == "OK"))) {
          EDDLOG("StfSender was NOT notified about transfer finish tf_builder={} tf_id={}", lTfBuilderId, lStfId);
        }
      } else {
        EDDLOG("StfSender could not transfer stf metadata to tf_builder={} tf_id={}", lTfBuilderId, lStfId);
      }
      lConnInfo = nullptr;
    } // Unlock TfBuilder to avoid serializing on STF destruction

    // send Stf to dealloc thread
    mStfDeleteQueue.push(std::move(lStf));

    // update buffer status
    lNumSentStfs += 1;

    StdSenderOutputCounters::Values lCounters;
    {
      std::scoped_lock lCntLock(mCounters.mCountersLock);
      mCounters.mValues.mBuffered.mSize -= lStfSize;
      mCounters.mValues.mBuffered.mCnt -= 1;
      mCounters.mValues.mInSending.mSize -= lStfSize;
      mCounters.mValues.mInSending.mCnt -= 1;
      mCounters.mValues.mTotalSent.mSize += lStfSize;
      mCounters.mValues.mTotalSent.mCnt += 1;

      lCounters = mCounters.mValues;
    }

    if (lCounters.mInSending.mCnt > 100) {
      DDDLOG_RL(2000, "DataHandlerThread: Number of buffered STFs. tfb_id={} num_stf_total={} size_stf_total={}",
        lTfBuilderId, lCounters.mInSending.mCnt, lCounters.mInSending.mSize);
    }

    DDMON("stfsender", "stf_output.stf_id", lStfId);
  }

  DDDLOG("Exiting StfSenderOutputUCX[{}]", pThreadIdx);
}

void StfSenderOutputUCX::StfDeallocThread()
{
  std::optional<std::unique_ptr<SubTimeFrame> > lStfOpt;
  while ((lStfOpt = mStfDeleteQueue.pop()) != std::nullopt) {

  }
}


} /* o2::DataDistribution */