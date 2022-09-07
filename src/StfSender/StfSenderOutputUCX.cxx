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
    lConnInfo->mOutputUCX.handle_client_ep_error(lConnInfo, status);
}

static ucs_status_t ucp_am_data_cb(void *arg, const void *header, size_t header_length,
                                  void *data, size_t length, const ucp_am_recv_param_t *param)
{
  StfSenderOutputUCX *lOutputUcx = reinterpret_cast<StfSenderOutputUCX*>(arg);
  (void) data;
  (void) length;
  (void) header_length;
  (void) param;

  assert (header_length == sizeof (std::uint64_t));

  std::uint64_t lAckStfId = 0;
  std::memcpy(&lAckStfId, header, sizeof (std::uint64_t));

  lOutputUcx->pushStfAck(lAckStfId);
  return UCS_OK;
}

StfSenderOutputUCX::StfSenderOutputUCX(std::shared_ptr<ConsulStfSender> pDiscoveryConfig, StdSenderOutputCounters &pCounters)
  : mDiscoveryConfig(pDiscoveryConfig),
    mStfSenderId(pDiscoveryConfig->status().info().process_id()),
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

  // first create all worker objects
  for (std::size_t i = 0; i < mThreadPoolSize; i++) {
    mDataWorkers.emplace_back();
    if (!ucx::util::create_ucp_worker(ucp_context, &mDataWorkers.back(), std::to_string(i))) {
      return false;
    }
    // register the am handler
    DDDLOG("ListenerThread: ucx::util::register_am_callback() ...");
    if (!ucx::util::register_am_callback(mDataWorkers.back(), ucx::io::AM_STF_ACK, ucp_am_data_cb, this)) {
      return false;
    }
  }

  // we can accept connections after all workers are created
  mRunning = true;

  // start thread pool
  for (std::size_t i = 0; i < mThreadPoolSize; i++) {
    std::string lThreadName = "stfs_ucx_" + std::to_string(i);
    mThreadPool.emplace_back(
      std::move(create_thread_member(lThreadName.c_str(), &StfSenderOutputUCX::DataHandlerThread, this, i))
    );
  }

  // start astf ack thread
  mStfAckThread = std::move(create_thread_member("stfs_ack", &StfSenderOutputUCX::StfAckThread, this));

  // start dealloc thread
  mDeallocThread = std::move(create_thread_member("stfs_dealloc", &StfSenderOutputUCX::StfDeallocThread, this));

  return true;
}

void StfSenderOutputUCX::stop()
{
  mRunning = false;

  // stop the thread pool
  mSendRequestQueue.stop();
  mStfDeleteQueue.stop();
  mStfAckQueue.stop();

  {
    std::scoped_lock lLockTfBuilders(mStfsInFlightMutex);
    mStfsInFlight.clear();
  }

  for (auto &lThread : mThreadPool) {
    if (lThread.joinable()) {
      lThread.join();
    }
  }
  mThreadPool.clear();

  if (mStfAckThread.joinable()) {
    mStfAckThread.join();
  }

  if (mDeallocThread.joinable()) {
    mDeallocThread.join();
  }
  DDDLOG("StfSenderOutputUCX::stop: stopped all threads.");

  // close all connections
  {
    std::unique_lock lLock(mOutputMapLock);
    for (auto &lConn : mOutputMap) {
      ucx::util::close_ep_connection(lConn.second->mWorker, lConn.second->ucp_ep);
    }
    mOutputMap.clear();
  }
  DDDLOG("StfSenderOutputUCX::stop: closed all ep");

  {// close all workers and the context
    for (auto &lWorker : mDataWorkers) {
      ucp_worker_destroy(lWorker.ucp_worker);
    }
  }
  DDDLOG("StfSenderOutputUCX::stop: closed all connections.");

  // Revoke all rkeys and mappings
  {
    std::scoped_lock lLock(mRegionListLock);
    for (auto &lMapping : mRegions) {
      ucx::util::destroy_rkey_for_region(ucp_context, lMapping.ucp_mem, lMapping.ucp_rkey_buf);
    }
  }

  ucp_cleanup(ucp_context);
  DDDLOG("StfSenderOutputUCX::stop: revoked all rkeys.");
}

ConnectStatus StfSenderOutputUCX::connectTfBuilder(const std::string &pTfBuilderId, const std::string &lTfBuilderIp, const unsigned lTfBuilderPort)
{
  DDDLOG("StfSenderOutputUCX::connectTfBuilder: transport starting for tfbuilder_id={}", pTfBuilderId);
  if (!mRunning.load()) {
    EDDLOG_ONCE("StfSenderOutputUCX::connectTfBuilder: backend is not started.");
    return eCONNERR;
  }

  // Check if connection already exists
  {
    std::shared_lock lLock(mOutputMapLock);

    if (mOutputMap.count(pTfBuilderId) > 0) {
      EDDLOG("StfSenderOutputUCX::connectTfBuilder: TfBuilder is already connected. tfb_id={}", pTfBuilderId);
      return eEXISTS;
    }
  }

  // use one of the configured workers for the TfBuilder connection
  static std::atomic_uint sCurrentWorkerIdx = 0;
  auto lWorkerIndex = (sCurrentWorkerIdx++) % mDataWorkers.size();

  auto lConnInfo = std::make_unique<StfSenderUCXConnInfo>(*this, mDataWorkers[lWorkerIndex], pTfBuilderId);

  // create endpoint for TfBuilder connection
  DDDLOG("Connect to TfBuilder ip={} port={}", lTfBuilderIp, lTfBuilderPort);
  DDDLOG("StfSenderOutputUCX::connectTfBuilder: ucx::util::create_ucp_client_ep ip={} port={}", lTfBuilderIp, lTfBuilderPort);
  if (!ucx::util::create_ucp_client_ep(lConnInfo->mWorker, lTfBuilderIp, lTfBuilderPort,
    &lConnInfo->ucp_ep, client_ep_err_cb, lConnInfo.get(), pTfBuilderId)) {

    return eCONNERR;
  }
  DDDLOG("StfSenderOutputUCX::connectTfBuilder: ucx::io::ucx_send_string ip={} port={}", lTfBuilderIp, lTfBuilderPort);
  const std::string lStfSenderId = mDiscoveryConfig->status().info().process_id();
  auto lOk = ucx::io::ucx_send_string(lConnInfo->mWorker, lConnInfo->ucp_ep, lStfSenderId);
  if (!lOk) {
    EDDLOG("connectTfBuilder: Sending of local id failed.");
    return eCONNERR;
  }

  // Add the connection to connection map
  {
    std::unique_lock lLock(mOutputMapLock);
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
  DDDLOG("StfSenderOutputUCX::disconnectTfBuilder: received for tf_builder={}", pTfBuilderId);

  // find and remove from the connection map
  std::unique_ptr<StfSenderUCXConnInfo> lConnInfo;
  {
    std::unique_lock lLock(mOutputMapLock);

    auto lIt = mOutputMap.find(pTfBuilderId);

    if (lIt == mOutputMap.end()) {
      DDDLOG("StfSenderOutputUCX::disconnectTfBuilder: TfBuilder was not connected. tfb_id={}", pTfBuilderId);
      {// mark the tfbuilder as disconnected. if there was a connection before, wait until ucx ep is destroyed
        std::scoped_lock lLockTfBuilders(mStfsInFlightMutex);
        mDisconnectedTfBuilders.insert(pTfBuilderId);
      }
      return true;
    }
    lConnInfo = std::move(mOutputMap.extract(lIt).mapped());
  }

  // Transport is only closed when other side execute close as well. Execute async
  std::thread([this, pConnInfo = std::move(lConnInfo), pTfBuilderId](){
    DDDLOG("StfSenderOutputUCX::disconnectTfBuilder: closing transport for tf_builder={}", pTfBuilderId);
    // acquire the lock and close the connection
    std::unique_lock lTfSenderLock(pConnInfo->mTfBuilderLock);
    ucx::util::close_ep_connection(pConnInfo->mWorker, pConnInfo->ucp_ep);
    DDDLOG("StfSenderOutputUCX::disconnectTfBuilder: transport stopped for tf_builder={}", pTfBuilderId);

    {// mark the tfbuilder as disconnected
      std::scoped_lock lLockTfBuilders(mStfsInFlightMutex);
      mDisconnectedTfBuilders.insert(pTfBuilderId);
    }

  }).detach();

  return true;
}

bool StfSenderOutputUCX::sendStfToTfBuilder(const std::string &pTfBuilderId, ScheduledStfInfo &&pStfInfo)
{
  mSendRequestQueue.push(std::make_unique<SendStfInfo>(std::move(pStfInfo.mStf), pTfBuilderId));
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
  lStfUCXMeta->set_stf_sender_id(mDiscoveryConfig->status().info().process_id());

  // pack all headers and collect data
  std::uint64_t lDataIovIdx = 0;

  for (auto& lDataIdentMapIter : pStf.mData) {
    for (auto& lSubSpecMapIter : lDataIdentMapIter.second) {
      for (auto& lStfDataIter : lSubSpecMapIter.second) {
        if (lStfDataIter.mHeader) {
          if (lStfDataIter.mDataParts.size() == 0) {
            EDDLOG_GRL(10000, "StfSenderOutput: Data format error: O2 header without data payload. Check the FLP-workflow");
            continue;
          }
          // add the header
          auto lHdrMeta = lStfUCXMeta->mutable_stf_hdr_meta()->add_stf_hdr_iov();
          lHdrMeta->set_hdr_data(lStfDataIter.mHeader->GetData(), lStfDataIter.mHeader->GetSize());
          lHdrMeta->set_num_data_parts(lStfDataIter.mDataParts.size());

          // add the data part info
          for (auto &lDataMsg : lStfDataIter.mDataParts) {
            auto &lDataPtr = *lStfUCXMeta->add_stf_data_iov();
            lDataPtr.set_idx(lDataIovIdx++);
            lDataPtr.set_len(lDataMsg->GetSize());
            lDataPtr.set_start(reinterpret_cast<std::uint64_t>(lDataMsg->GetData()));
            lDataPtr.set_txg(std::uint32_t(-1));
          }
        }
      }
    }
  }

  // Only create transactions if there is actual data.
  // Note that an STF can have all o2 messages of size zero, but we dont need txgs for them
  if (lStfSize > 0) {
    std::size_t lTotalGap = 0;

    assert (!lStfUCXMeta->stf_data_iov().empty());
    // sort all data parts by pointer value in order to make txgs
    std::sort(lStfUCXMeta->mutable_stf_data_iov()->begin(), lStfUCXMeta->mutable_stf_data_iov()->end(),
      [](const UCXData &a, const UCXData &b) { return a.start() < b.start(); } );

    // apparently shm ptr can be null. Otherwise they must be part of a registered region.
    // After sorting, null pointers will come to the front of the data array.
    // We need to associate them with the first txg, and not call regionLookup
    // NOTE: on EPN such messages will not have null ptr value, but a value within TF buffer (still zero size)
    // skip all null pointers

    // move all zero length buffers to the beginning and get the first non-zero buffer iterator
    auto lUCXData = std::stable_partition(lStfUCXMeta->mutable_stf_data_iov()->begin(), lStfUCXMeta->mutable_stf_data_iov()->end(),
      [](const UCXData &a) { return ((a.len() == 0) || (a.start() == 0)); } );


    if (lUCXData != lStfUCXMeta->mutable_stf_data_iov()->end()) {

      // create the first transaction
      std::uint32_t lTxgIdx = 0;
      std::uint32_t lRegionIdx = 0;

      UCXIovTxg *lStfTxg = lStfUCXMeta->add_stf_txg_iov();
      lStfTxg->set_data_parts(0);
      lStfTxg->set_start(0);
      lStfTxg->set_len(0);

      assert (lUCXData->len() > 0);
      const UCXMemoryRegionInfo* lRunningRegion = regionLookup(lUCXData->start(), lUCXData->len());
      // create the first region
      UCXRegion *lStfRegion = lStfUCXMeta->add_data_regions();
      lStfRegion->set_region(lRegionIdx++);
      lStfRegion->set_size(lRunningRegion->mSize);
      lStfRegion->set_region_rkey(lRunningRegion->ucp_rkey_buf, lRunningRegion->ucp_rkey_buf_size);

      // Update the start to region offset
      lUCXData->set_txg(lTxgIdx);

      // create the first txg
      lStfTxg->set_txg(lTxgIdx++);
      lStfTxg->set_start(lUCXData->start());
      lStfTxg->set_len(lUCXData->len());
      lStfTxg->set_region(lStfRegion->region());

      // iterate over non-zero messages
      for (++lUCXData; lUCXData != lStfUCXMeta->mutable_stf_data_iov()->end(); ++lUCXData) {

        assert (lUCXData->start() != 0);
        assert (lUCXData->len() > 0);

        const auto lReg = regionLookup(lUCXData->start(), lUCXData->len());

        if (*lReg == *lRunningRegion) {
          // check if we can extend the current txg
          const auto lGap = lUCXData->start() - (lStfTxg->start() + lStfTxg->len());
          if (lGap <= mRmaGap) {
            // extend the existing txg
            lStfTxg->set_len(lStfTxg->len() + lGap + lUCXData->len());
            lStfTxg->set_data_parts(lStfTxg->data_parts() + 1);
            lTotalGap += lGap;
            // set the data part txg
            lUCXData->set_txg(lStfTxg->txg());

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
        lStfTxg->set_start(lUCXData->start());
        lStfTxg->set_len(lUCXData->len());
        lStfTxg->set_data_parts(1);
        lStfTxg->set_region(lStfRegion->region());
        // set the data part txg
        lUCXData->set_txg(lStfTxg->txg());
      }
    }

    DDDLOG_GRL(10000, "UCX pack total data_size={} data_cnt={} txg_size={} txg_cnt={} gap_size={}",
      pStf.getDataSize(), lStfUCXMeta->stf_data_iov_size(), pStf.getDataSize()+lTotalGap, lStfUCXMeta->stf_txg_iov_size(), lTotalGap);

    DDMON("stfsender", "ucx_txg.count", lStfUCXMeta->stf_txg_iov_size());
    DDMON("stfsender", "ucx_buffer.count", lStfUCXMeta->stf_data_iov_size());
    DDMON("stfsender", "ucx_rma_gap_total.size", lTotalGap);
    if (lStfSize > 0) {
      DDMON("stfsender", "ucx_rma_gap_overhead", (double(lTotalGap) / double(lStfSize) * 100.));
    }
  }
}

void StfSenderOutputUCX::prepareStfMetaHeader(const SubTimeFrame &pStf, UCXIovStfHeader *pStfUCXMeta)
{
  pStf.accept(*this, pStfUCXMeta);
}

/// prepare ucx metadata for stfs
void StfSenderOutputUCX::DataHandlerThread(unsigned pThreadIdx)
{
  DDDLOG("StfSenderOutputUCX: Starting meta thread {}", pThreadIdx);

#if defined(__linux__)
  if (nice(-10)) { }
#endif

  // local worker we advance here
  assert (pThreadIdx < mDataWorkers.size());
  ucx::dd_ucp_worker &lWorker = mDataWorkers[pThreadIdx];
  UCXIovStfHeader lStfMeta;

  while (mRunning) {
    std::optional<std::unique_ptr<SendStfInfo>> lSendReqOpt = mSendRequestQueue.pop_wait_for(5ms);
    while (ucp_worker_progress(lWorker.ucp_worker) > 0) { }
    if (!lSendReqOpt) {
      continue;
    }

    std::unique_ptr<SendStfInfo> lSendReq = std::move(lSendReqOpt.value());
    auto &lStf = lSendReq->mStf;
    const auto lStfId = lStf->id();
    lStfMeta.Clear();

    const auto &lTfBuilderId = lSendReq->mTfBuilderId;

    // pack the ucx meta
    prepareStfMetaHeader(*lStf, &lStfMeta);

    // send the meta to tfbuilder
    StfSenderUCXConnInfo *lConnInfo;
    {
      // get the thread data.
      std::shared_lock lLock(mOutputMapLock);
      if (mOutputMap.count(lTfBuilderId) == 1) {
        lConnInfo = mOutputMap[lTfBuilderId].get();

        if (lConnInfo && lConnInfo->mConnError) {
          EDDLOG_GRL(1000, "StfSenderOutputUCX: Skipping sending data because of peer connection issues. tfbuilder_id={}", lConnInfo->mTfBuilderId);
          mStfDeleteQueue.push(std::move(lStf));
          continue;
        }
      } else {
        mStfDeleteQueue.push(std::move(lStf));
        continue;
      }
    }

    {
      { // store the request before we notify tfbuilder to avoid race on freeing
        std::scoped_lock lLock(mStfsInFlightMutex);
        mStfsInFlight[lStfId] = std::move(lSendReq);
      }

      std::string lStfMetaData = lStfMeta.SerializeAsString();

      DDMON_RATE("stfsender", "ucx_meta", lStfMetaData.size());

      if (!ucx::io::ucx_send_am(lConnInfo->mWorker, lConnInfo->ucp_ep, ucx::io::AM_STF_META, lStfMetaData.data(), lStfMetaData.size())) {
        EDDLOG("StfSender could not transfer stf metadata to tf_builder={} tf_id={}", lTfBuilderId, lStfId);
        {
          std::scoped_lock lLock(mStfsInFlightMutex);
          mStfDeleteQueue.push(std::move(mStfsInFlight[lStfId]->mStf));
        }
      }
    }
  }

  DDDLOG("StfSenderOutputUCX: exiting meta thread {}", pThreadIdx);
}

void StfSenderOutputUCX::StfAckThread()
{
  DDMON_RATE("stfsender", "stf_output", 0.0);

  while (mRunning) {
    std::optional<std::uint64_t> lStfAckOpt = mStfAckQueue.pop_wait_for(100ms);

     auto lDeallocStfInfo = [this](std::unique_ptr<SendStfInfo> &&pStfInfo, const bool pSent) -> void {
      // all is well remove
      assert (pStfInfo);
      auto &lStf = pStfInfo->mStf;
      const auto lStfSize = lStf->getDataSize();

      DDMON_RATE("stfsender", "stf_output", lStfSize);


      { // update the counters
        std::scoped_lock lCntLock(mCounters.mCountersLock);
        mCounters.mValues.mBuffered.mSize -= lStfSize;
        mCounters.mValues.mBuffered.mCnt -= 1;
        mCounters.mValues.mInSending.mSize -= lStfSize;
        mCounters.mValues.mInSending.mCnt -= 1;
        if (pSent) {
          mCounters.mValues.mTotalSent.mSize += lStfSize;
          mCounters.mValues.mTotalSent.mCnt += 1;
        }
      }
      // send for dealloc
      mStfDeleteQueue.push(std::move(lStf));
    };

    // deallocate finished stf
    if (lStfAckOpt.has_value()) {
      // extract the stf info from the map
      std::unique_ptr<SendStfInfo> lStfInfo = nullptr;
      const auto lStfId = lStfAckOpt.value();

      {
        std::scoped_lock lLock(mStfsInFlightMutex);
        auto lStfInfoIt = mStfsInFlight.find(lStfId);
        if (lStfInfoIt != mStfsInFlight.end()) {
          lStfInfo = std::move(lStfInfoIt->second);
          mStfsInFlight.erase(lStfInfoIt);
        }
      }

      if (lStfInfo) {
        lDeallocStfInfo(std::move(lStfInfo), true);
      }
    }

    {// check if the oldest stf should be removed because tfbuilder disconnected
      // extract the stf info from the map
      std::unique_ptr<SendStfInfo> lStfInfo = nullptr;
      {
        std::scoped_lock lLock(mStfsInFlightMutex);

        if (!mStfsInFlight.empty()) {
          const auto &lTfBuilderId = mStfsInFlight.begin()->second->mTfBuilderId;

          if (mDisconnectedTfBuilders.count(lTfBuilderId) > 0) {

            lStfInfo = std::move(mStfsInFlight.begin()->second);
            mStfsInFlight.erase(mStfsInFlight.cbegin());

          }
        }
      }
      if (lStfInfo) {
        lDeallocStfInfo(std::move(lStfInfo), false);
      }
    }
  }
}

void StfSenderOutputUCX::StfDeallocThread()
{
  std::optional<std::unique_ptr<SubTimeFrame> > lStfOpt;
  while ((lStfOpt = mStfDeleteQueue.pop()) != std::nullopt) {
    // intentionally left blank
  }
}

} /* o2::DataDistribution */