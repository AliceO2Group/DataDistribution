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

#include "TfBuilderInputUCX.h"
#include "TfBuilderInputDefs.h"
#include "TfBuilderRpc.h"

#include <TfSchedulerRpcClient.h>

#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameVisitors.h>

#include <DataDistMonitoring.h>

#include <UCXSendRecv.h>

#include <boost/algorithm/string/join.hpp>

#include <condition_variable>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <chrono>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

static void client_ep_err_cb(void *arg, ucp_ep_h, ucs_status_t status)
{
  TfBuilderUCXConnInfo *lConnInfo = reinterpret_cast<TfBuilderUCXConnInfo*>(arg);
  if (lConnInfo) {
    lConnInfo->mInputUCX.handle_client_ep_error(lConnInfo, status);
  }
}

static void listen_conn_handle_cb(ucp_conn_request_h conn_request, void *arg)
{
  dd_ucp_listener_context_t *lCtx = reinterpret_cast<dd_ucp_listener_context_t*>(arg);
  lCtx->mInputUcx->new_conn_handle(conn_request);
}

static ucs_status_t ucp_am_data_cb(void *arg, const void *header, size_t header_length,
                            void *data, size_t length, const ucp_am_recv_param_t *param)
{
  TfBuilderInputUCX *lInputUcx = reinterpret_cast<TfBuilderInputUCX*>(arg);
  (void) header;
  (void) header_length;

  const auto lMetaDecodeStart = std::chrono::steady_clock::now();

  if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) {
    /* Rendezvous request arrived, data contains an internal UCX descriptor,
      * which has to be passed to ucp_am_recv_data_nbx function to confirm
      * data transfer.
      */

    EDDLOG_RL(10000, "UCX: Unexpected ucp_am_data_cb RNDV stf_meta_size={}", length);
    return UCS_OK;
  }

  // Translate data to UCX metadata message and queue
  UCXIovStfHeader lMeta;
  if (lInputUcx->createMetadata(data, length, lMeta)) {
    lInputUcx->pushMetadata(std::move(lMeta));
  }
  DDMON("tfbuilder", "recv.meta_decode_ms", since<std::chrono::milliseconds>(lMetaDecodeStart));
  return UCS_OK;
}

// callback on new connection
void TfBuilderInputUCX::new_conn_handle(ucp_conn_request_h conn_request)
{
  ucp_conn_request_attr_t attr;

  attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
  auto lStatus = ucp_conn_request_query(conn_request, &attr);
  if (lStatus != UCS_OK) {
    DDDLOG("Failed to query the connection request. error={}", std::string(ucs_status_string(lStatus)));
    return;
  }

  const auto lStfSenderAddr = ucx::util::sockaddr_to_string(&attr.client_address);
  const auto lStfSenderPort = ucx::util::sockaddr_to_port(&attr.client_address);

  DDDLOG("Received a connection request! addr={} port={}", lStfSenderAddr, lStfSenderPort);

  if (mState.load() != CONFIGURING) {
    EDDLOG("Received a connection request but not in CONFIGURING state. state={}", mState.load());
    return;
  }

  // forward to Listener thread
  mConnRequestQueue.push(lStfSenderAddr, lStfSenderPort, conn_request);
}

void TfBuilderInputUCX::ListenerThread()
{
  const auto lListenerStart = std::chrono::steady_clock::now();

  while(mState.load() == CONFIGURING && !mConnectionIssues) {
    // progress the listener worker
    while (ucp_worker_progress(listener_worker.ucp_worker) > 0) { }

    auto lConnInfoOpt = mConnRequestQueue.pop_wait_for(50ms);
    if (!lConnInfoOpt.has_value()) {
      // complain about missing connections
      std::size_t lNumConnected = 0;
      std::set<std::string> lMissingStfSenderIds = mExpectedStfSenderIds;
      {
        std::unique_lock lLock(mConnectionMapLock);
        lNumConnected = mConnMap.size();
        for (const auto &lConn : mConnMap) {
          lMissingStfSenderIds.erase(lConn.first);
        }
      }

      if (lNumConnected < mNumStfSenders) {
        if (since<std::chrono::seconds>(lListenerStart) > 5.0) {
          IDDLOG_RL(5000, "TfBuilderInputUCX: Missing connections from StfSender. missing_cnt={} missing_stfsenders={}",
            (mNumStfSenders - lNumConnected), boost::algorithm::join(lMissingStfSenderIds, ","));
        }
      }
      continue;
    }

    // we have a connection request
    assert (lConnInfoOpt.has_value());
    const auto lStfSenderAddr = std::get<0>(lConnInfoOpt.value());
    ucp_conn_request_h conn_request = std::get<2>(lConnInfoOpt.value());

    // use one of the configured workers for the TfBuilder connection
    static std::atomic_uint sCurrentWorkerIdx = 0;
    const auto lWorkerIndex = (sCurrentWorkerIdx++) % mDataWorkers.size();

    // Create stfsender (data) worker + endpoint
    auto lConnStruct = std::make_unique<TfBuilderUCXConnInfo>(*this, mDataWorkers[lWorkerIndex]);
    lConnStruct->mStfSenderIp = lStfSenderAddr;

    // Create stfsender endpoint
    DDDLOG("ListenerThread: ucx::util::create_ucp_ep() ...");
    if (!ucx::util::create_ucp_ep(lConnStruct->mWorker.ucp_worker, conn_request, &lConnStruct->ucp_ep,
      client_ep_err_cb, lConnStruct.get(), lStfSenderAddr)) {
      continue;
    }

    // receive the StfSenderId
    DDDLOG("ListenerThread: ucx::util::ucx_receive_string() ...");
    auto lStfSenderIdOpt = ucx::io::ucx_receive_string(lConnStruct->mWorker);

    if (!lStfSenderIdOpt) {
      EDDLOG("ListenerThread: Connection request: Failed to receive StfSenderId");
      continue;
    }

    const auto &lStfSenderId = lStfSenderIdOpt.value();
    DDDLOG("UCXListenerThread::Connection request. stf_sender_id={}", lStfSenderId);
    lConnStruct->mStfSenderId = lStfSenderId;

    { // create the meta queue mapping to the selected worker
      std::unique_lock lLock(mStfMetaWorkerQueuesMutex);
      mStfMetaWorkerQueues[lStfSenderId] = mDataWorkersQueues[lWorkerIndex];
      mStfSenderToWorkerMap[lStfSenderId] = lWorkerIndex;
    }

    // add the connection info map
    std::unique_lock lLock(mConnectionMapLock);
    assert (mConnMap.count(lStfSenderId) == 0);
    mConnMap[lStfSenderId] = std::move(lConnStruct);
  }

  DDDLOG("TfBuilderInputUCX: Listener thread stopped.");
}

bool TfBuilderInputUCX::start()
{
  // setting configuration options
  mThreadPoolSize = std::clamp(mConfig->getUInt64Param(UcxTfBuilderThreadPoolSizeKey, UcxTfBuilderThreadPoolSizeDefault), std::size_t(1), std::size_t(256));
  mRdmaPollingWait =mConfig->getBoolParam(UcxPollForRDMACompletionKey, UcxPollForRDMACompletionDefault);
  IDDLOG("TfBuilderInputUCX: Configuration loaded. thread_pool={} polling={}", mThreadPoolSize, mRdmaPollingWait);

  auto &lConfStatus = mConfig->status();

  // get list of stfsender ids
  for (const auto &lStfSenderId : mRpc->getStfSenderIds()) {
    mExpectedStfSenderIds.insert(lStfSenderId);
  }
  IDDLOG("TfBuilderInputUCX: Expecting connections from StfSenders. stf_sender_cnt={}", mExpectedStfSenderIds.size());

  // disabled until the listener is initialized
  lConfStatus.mutable_ucx_info()->set_enabled(false);

  // Init and create UCX context
  if (!ucx::util::create_ucp_context(&ucp_context)) {
    return false;
  }

  // create all data workers
  for (unsigned i = 0; i < mThreadPoolSize; i++) {
    mDataWorkers.emplace_back();
    if (!ucx::util::create_ucp_worker(ucp_context, &mDataWorkers.back(), std::to_string(i))) {
      return false;
    }
    // register the am handler
    DDDLOG("ListenerThread: ucx::util::register_am_callback() ...");
    if (!ucx::util::register_am_callback(mDataWorkers.back(), ucx::io::AM_STF_META, ucp_am_data_cb, this)) {
      return false;
    }

    // create worker queues
    mDataWorkersQueues.emplace_back(std::make_shared<ConcurrentQueue<StfMetaRdmaInfo>>());
  }

  // Create listener worker for accepting connections from StfSender
  if (!ucx::util::create_ucp_worker(ucp_context, &listener_worker, "Listener")) {
    return false;
  }

  // Create the listener
  // Run the connection callback with pointer to us
  dd_ucp_listen_context.mInputUcx = this;
  // Get IPv4 address for listener from the configuration
  const auto &lIpAaddress = lConfStatus.info().ip_address();

  if (!ucx::util::create_ucp_listener(listener_worker.ucp_worker, lIpAaddress, &ucp_listener,
    listen_conn_handle_cb, &dd_ucp_listen_context)) {
    return false;
  }

  // Start the Listener thread
  mListenerThread = create_thread_member("ucx_listener", &TfBuilderInputUCX::ListenerThread, this);

  // Publish the address on which the listener accepts connections
  {
    ucp_listener_attr_t attr;
    attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
    const auto lStatus = ucp_listener_query(ucp_listener, &attr);
    if (lStatus != UCS_OK) {
      EDDLOG("Failed to query th UCX listener {}", std::string(ucs_status_string(lStatus)));
      ucp_listener_destroy(ucp_listener);
      return false;
    }
    // extract the port
    const auto lListenPort = ucx::util::sockaddr_to_port(&attr.sockaddr);

    lConfStatus.mutable_ucx_info()->mutable_listen_ep()->set_ip(lConfStatus.info().ip_address());
    lConfStatus.mutable_ucx_info()->mutable_listen_ep()->set_port(lListenPort);
    lConfStatus.mutable_ucx_info()->set_enabled(true);

    if (mConfig->write()) {
      IDDLOG("TfBuilder UCX listener ip={} port={}", lConfStatus.info().ip_address(), lListenPort);
    } else {
      IDDLOG("TfBuilder UCX listener created. Discovery configuration writing failed!");
      ucp_listener_destroy(ucp_listener);
      return false;
    }
  }

  // Get number of StfSenders in the partition
  if (!mRpc->TfSchedRpcCli().NumStfSendersInPartitionRequest(mNumStfSenders)) {
    WDDLOG_RL(5000, "gRPC error: cannot reach scheduler. scheduler_ep={}", mRpc->TfSchedRpcCli().getEndpoint());
    return false;
  }

  if (mNumStfSenders == 0 || mNumStfSenders == std::uint32_t(-1)) {
    EDDLOG("gRPC error: number of StfSenders in partition: {}." , mNumStfSenders);
    return false;
  }

  // Connect all StfSenders
  TfBuilderUCXConnectionResponse lConnResult;
  do {
    IDDLOG_RL(5000, "Requesting StfSender connections from the TfScheduler.");

    lConnResult.Clear();
    if (!mRpc->TfSchedRpcCli().TfBuilderUCXConnectionRequest(lConfStatus, lConnResult)) {
      EDDLOG("Input::start: RPC error: Request for StfSender connection failed.");
      return false;
    }

    if (lConnResult.status() == ERROR_STF_SENDERS_NOT_READY) {
      DDDLOG_RL(5000, "Input::start: StfSenders are not ready. Retrying...");
      std::this_thread::sleep_for(250ms);
      continue;
    }

    if (lConnResult.status() == ERROR_PARTITION_TERMINATING) {
      IDDLOG_RL(5000, "Input::start: Partition is terminating. Stopping...");
      return false;
    }

    if (lConnResult.status() != OK) {
      EDDLOG("Input::start: Request for StfSender connection failed. scheduler_error={}",
        TfBuilderConnectionStatus_Name(lConnResult.status()));
      return false;
    }

    // connection successful
    break;

  } while(true);

  // Wait until we have all endpoints for StfSenders
  do {
    std::size_t lNumConnected = 0;
    {
      std::shared_lock lLock(mConnectionMapLock);
      lNumConnected = mConnMap.size();
    }

    if (lNumConnected == mNumStfSenders) {
      break;
    }

    std::this_thread::sleep_for(100ms);
    DDDLOG_RL(5000, "TfBuilderInputUCX::start: Waiting for all StfSender ucx endpoints. connected={} total={}", lNumConnected, mNumStfSenders);
  } while (!mConnectionIssues);

  // This will stop the Listener thread
  mState = RUNNING;

  if (mConnectionIssues) {
    EDDLOG("TfBuilderInputUCX::start: Connection issues. Exiting.");
    return false;
  }

  DDDLOG("TfBuilderInputUCX::start: Finished");
  return true;
}

bool TfBuilderInputUCX::map_data_region()
{
  // map the receive buffer for ucx rma
  ucp_data_region_set = false;
  const auto lOrigAddress = mTimeFrameBuilder.mMemRes.mDataMemRes->address();
  const auto lOrigSize = mTimeFrameBuilder.mMemRes.mDataMemRes->size();
  ucx::util::create_rkey_for_region(ucp_context, lOrigAddress, lOrigSize, /*rw*/ false,  &ucp_data_region, nullptr, nullptr);

  auto lUcxMemPtr = ucx::util::get_mem_address(ucp_data_region);
  if (!lUcxMemPtr) {
    EDDLOG("TfBuilderInputUCX::start: Failed to map TF region with UCX.");
    return false;
  }
  // set the UCX pointer for the region
  mTimeFrameBuilder.mMemRes.mDataMemRes->set_ucx_address(lUcxMemPtr);
  ucp_data_region_set = true;
  DDDLOG("TfBuilderInputUCX::map_data_region(): mapped the data region size={}", lOrigSize);

  // start receiving thread pool
  // NOTE: This must come after the region mapping. Threads are using mapped addresses
  for (unsigned i = 0; i < mThreadPoolSize; i++) {
    { // preprocess threads
      std::string lThreadName = "tfb_ucx_prep_" + std::to_string(i);
      mPrepThreadPool.emplace_back(std::move(
        create_thread_member(lThreadName.c_str(), &TfBuilderInputUCX::StfPreprocessThread, this, i))
      );
    }

    { // rma get threads
      std::string lThreadName = "tfb_ucx_rdma_" + std::to_string(i);
      mThreadPool.emplace_back(std::move(
        create_thread_member(lThreadName.c_str(), &TfBuilderInputUCX::DataHandlerThread, this, i))
      );
    }

    { // postprocess threads
      std::string lThreadName = "tfb_ucx_post_" + std::to_string(i);
      mPrepThreadPool.emplace_back(std::move(
        create_thread_member(lThreadName.c_str(), &TfBuilderInputUCX::StfPostprocessThread, this, i))
      );
    }
  }

  return true;
}

void TfBuilderInputUCX::stop()
{
  // first stop accepting TimeFrames
  mRpc->stopAcceptingTfs();

  mState = TERMINATED;

  if (mListenerThread.joinable()) {
    mListenerThread.join();
  }
  DDDLOG("TfBuilderInputUCX::stop: Listener thread stopped.");

  // Wait for input threads to stop
  DDDLOG("TfBuilderInputUCX::stop: Waiting for input threads to terminate.");
  mStfReqQueue.stop();
  mStfPreprocessQueue.stop();
  mStfPostprocessQueue.stop();

  for (auto &lQueue : mDataWorkersQueues) {
    lQueue->stop();
  }

  for (auto& lIdThread : mPrepThreadPool) {
    if (lIdThread.joinable())
      lIdThread.join();
  }

  for (auto& lIdThread : mThreadPool) {
    if (lIdThread.joinable())
      lIdThread.join();
  }

  for (auto& lIdThread : mPostThreadPool) {
    if (lIdThread.joinable())
      lIdThread.join();
  }

  mPrepThreadPool.clear();
  mThreadPool.clear();
  mPostThreadPool.clear();
  DDDLOG("TfBuilderInputUCX::stop: All input threads terminated.");

  // Disconnect all input channels
  // RPC: Send disconnect request to scheduler
  {
    StatusResponse lResult;
    auto &lStatus = mConfig->status();
    if (mRpc->TfSchedRpcCli().TfBuilderUCXDisconnectionRequest(lStatus, lResult)) {
      DDDLOG("TfBuilderInputUCX::stop: RPC Request for StfSender disconnect successful.");
    } else {
      DDDLOG("TfBuilderInputUCX::stop: RPC error: Request for StfSender disconnect failed!");
    }
  }

  // unmap the receive buffers
  if (ucp_data_region_set) {
    // remove the ucx mapping from the region
    void* lOrigAddress = mTimeFrameBuilder.mMemRes.mDataMemRes->address();
    mTimeFrameBuilder.mMemRes.mDataMemRes->set_ucx_address(lOrigAddress);
    // unmap
    ucp_mem_unmap(ucp_context, ucp_data_region);
  }

  // close ucx: destroy remote rma keys and disconnect
  {
    std::unique_lock lLock(mConnectionMapLock);

    for (auto & lConn : mConnMap) {
      std::unique_lock lIoLock(lConn.second->mRemoteKeysLock);
      for (auto & lRKeyIt : lConn.second->mRemoteKeys) {
        ucp_rkey_destroy(lRKeyIt.second);
      }
      ucx::util::close_ep_connection(lConn.second->mWorker, lConn.second->ucp_ep);
    }
    mConnMap.clear();
  }

  {// close the listener and all workers
    ucp_listener_destroy(ucp_listener);
    ucp_worker_destroy(listener_worker.ucp_worker);
    for (auto &lWorker : mDataWorkers) {
      ucp_worker_destroy(lWorker.ucp_worker);
    }
    ucp_cleanup(ucp_context);
  }

  DDDLOG("TfBuilderInputUCX::stop: All input channels are closed.");
}

/// Receive buffer allocation thread
void TfBuilderInputUCX::StfPreprocessThread(const unsigned pThreadIdx)
{
  using clock = std::chrono::steady_clock;
  DDDLOG("Starting ucx preprocess thread {}", pThreadIdx);

  std::vector<std::uint64_t> lTxgSizes(16 << 10);

  std::optional<UCXIovStfHeader> lStfMetaOpt;
  while ((lStfMetaOpt = mStfPreprocessQueue.pop()) != std::nullopt) {

    UCXIovStfHeader lStfMeta = std::move(lStfMetaOpt.value());
    StfMetaRdmaInfo lStfRdmaInfo;

    lTxgSizes.clear();

    const auto lAllocStart = clock::now();

    // Allocate data memory for txgs
    using UCXIovTxg = UCXIovStfHeader::UCXIovTxg;

    std::for_each(lStfMeta.stf_txg_iov().cbegin(), lStfMeta.stf_txg_iov().cend(), [&lTxgSizes](const UCXIovTxg &txg) {
      lTxgSizes.push_back(txg.len());
    });

    lStfRdmaInfo.mTxgPtrs.reserve(lTxgSizes.size());

    mTimeFrameBuilder.allocDataBuffers(lTxgSizes, lStfRdmaInfo.mTxgPtrs);
    assert (lStfRdmaInfo.mTxgPtrs.size() == std::size_t(lStfMeta.stf_txg_iov().size()));

    DDMON("tfbuilder", "recv.data_alloc_ms", since<std::chrono::milliseconds>(lAllocStart));

    { // make sure all remote keys are unpacked
      // We make this in two passes in order to minimize impact on the RDMA thread.
      // Only take writer key lock if a region needs to be mapped.
      // This should only happen at the beginning.
      // NOTE: Region mapping could be moved to the init handshake, but it's not certain that sender knows about all regions at that time...

      const std::string &lStfSenderId = lStfMeta.stf_sender_id();
      // Reference to the input channel
      TfBuilderUCXConnInfo *lConn = nullptr;

      std::shared_lock lLock(mConnectionMapLock);
      if (mConnMap.count(lStfSenderId) == 1) {
        lConn = mConnMap.at(lStfSenderId).get();
        if (lConn && lConn->mConnError) {
          lConn = nullptr; // we don't do anything if error is signalled
        }
      }

      bool lAllMapped = true;
      {// check if all regions are mapped (reding mode)
        std::shared_lock lKeysLock(lConn->mRemoteKeysLock);

        for (const auto &lRegion : lStfMeta.data_regions()) {
          if (lConn->mRemoteKeys.count(lRegion.region_rkey()) == 0) {
            lAllMapped = false;
            break;
          }
        }
      }

      if (!lAllMapped) {// make sure all regions are mapped (writer mode)
        std::unique_lock lKeysLock(lConn->mRemoteKeysLock);

        for (const auto &lRegion : lStfMeta.data_regions()) {
          if (lConn->mRemoteKeys.count(lRegion.region_rkey()) == 0) {
            DDDLOG("UCX: Mapping a new remote region stf_sender={} size={}", lStfSenderId, lRegion.size());
            auto lNewRkeyIter = lConn->mRemoteKeys.emplace(lRegion.region_rkey(), ucp_rkey_h());
            ucp_ep_rkey_unpack(lConn->ucp_ep, lRegion.region_rkey().data(), &(lNewRkeyIter.first->second));
          }
        }
      }
    }

    lStfRdmaInfo.mStfMeta = std::move(lStfMeta);

    pushRdmaInfo(std::move(lStfRdmaInfo));
  }

  DDDLOG("Exiting ucx preprocess thread {}", pThreadIdx);
}

/// Receiving thread
void TfBuilderInputUCX::DataHandlerThread(const unsigned pThreadIdx)
{
  using clock = std::chrono::steady_clock;

  DDDLOG("Starting receiver thread[{}]", pThreadIdx);
  // Deserialization object (stf ID)
  IovDeserializer lStfReceiver(mTimeFrameBuilder);

  // memory for meta-tag receive
  const std::uint64_t lMetaMemSize = 1024;
  FairMQMessagePtr lMetaMemMsg = mTimeFrameBuilder.newDataMessage(lMetaMemSize);
  void *lMetaMemPtr = mTimeFrameBuilder.mMemRes.mDataMemRes->get_ucx_ptr(lMetaMemMsg->GetData());

  // local worker we advance here
  assert (pThreadIdx < mDataWorkers.size());
  ucx::dd_ucp_worker &lWorker = mDataWorkers[pThreadIdx];
  // request queue for the local worker
  assert (pThreadIdx < mDataWorkersQueues.size());
  auto lStfMetaQueue = mDataWorkersQueues[pThreadIdx];

  while (mState != TERMINATED) {
    std::optional<StfMetaRdmaInfo> lStfMetaOpt = lStfMetaQueue->pop_wait_for(5ms);
    if (!lStfMetaOpt) {
      while (ucp_worker_progress(lWorker.ucp_worker) > 0) { }
      continue;
    }

    const auto lRmaGetStart = clock::now();

    StfMetaRdmaInfo lStfRdmaInfo = std::move(lStfMetaOpt.value());
    UCXIovStfHeader &lStfMeta = lStfRdmaInfo.mStfMeta;
    const std::uint64_t lTfId = lStfMeta.stf_hdr_meta().stf_id();
    const std::string &lStfSenderId = lStfMeta.stf_sender_id();
    const auto &lTxgPtrs = lStfRdmaInfo.mTxgPtrs;

    // Reference to the input channel
    TfBuilderUCXConnInfo *lConn = nullptr;
    std::shared_lock lLock(mConnectionMapLock);

    if (mConnMap.count(lStfSenderId) == 1) {
      lConn = mConnMap.at(lStfSenderId).get();
      if (lConn && lConn->mConnError) {
        continue; // we are stoping
      }
    } else {
      continue;
    }

    {
      std::scoped_lock lStfSenderIoLock(lConn->mStfSenderIoLock);

      if (!lStfMeta.stf_txg_iov().empty()) {
        ucx::io::dd_ucp_multi_req_v2 lRmaReqSem;
        {
          // It's safe to use shared key lock because preprocess thread created required keys for this stf
          std::shared_lock lKeysLock(lConn->mRemoteKeysLock);

          // RMA get all the txgs
          for (const auto &lStfTxg : lStfMeta.stf_txg_iov()) {
            assert (lStfTxg.len() > 0);

            void *lTxgUcxPtr = mTimeFrameBuilder.mMemRes.mDataMemRes->get_ucx_ptr(lTxgPtrs[lStfTxg.txg()]);
            const ucp_rkey_h lRemoteKey = lConn->mRemoteKeys[lStfMeta.data_regions(lStfTxg.region()).region_rkey()];
            ucx::io::get(lConn->ucp_ep, lTxgUcxPtr, lStfTxg.len(), lStfTxg.start(), lRemoteKey, &lRmaReqSem);
          }
        }

        // wait for final completion
        if (!lRmaReqSem.wait(lConn->mWorker, mRdmaPollingWait)) {
            break;
        }
      }

      // notify StfSender we completed (use mapped scratch memory)
      std::uint64_t *lAckMsgStfId = reinterpret_cast<std::uint64_t *>(lMetaMemPtr);
      *lAckMsgStfId = lTfId;

      if (!ucx::io::ucx_send_am_hdr(lConn->mWorker, lConn->ucp_ep, ucx::io::AM_STF_ACK, lAckMsgStfId, sizeof(std::uint64_t)) ) {
        EDDLOG_GRL(10000, "StfSender was NOT notified about transfer finish stf_sender={} tf_id={}", lStfSenderId, lTfId);
      }
    }

    lStfRdmaInfo.mRdmaTimeMs = since<std::chrono::milliseconds>(lRmaGetStart);

    // RDMA DONE: send to post-processing
    pushPostprocessMetadata(std::move(lStfRdmaInfo));
  }

  DDDLOG("Exiting UCX input thread[{}]", pThreadIdx);
}


/// FMQ message creating thread
void TfBuilderInputUCX::StfPostprocessThread(const unsigned pThreadIdx)
{
  using clock = std::chrono::steady_clock;

  DDDLOG("Starting ucx postprocess thread {}", pThreadIdx);
  // Deserialization object (stf ID)
  IovDeserializer lStfReceiver(mTimeFrameBuilder);

  std::vector<std::pair<void*, std::size_t>> lDataMsgsBuffers;

  std::optional<StfMetaRdmaInfo> lStfRdmaInfoOpt;
  while ((lStfRdmaInfoOpt = mStfPostprocessQueue.pop()) != std::nullopt) {

    StfMetaRdmaInfo lStfRdmaInfo = std::move(lStfRdmaInfoOpt.value());
    UCXIovStfHeader &lStfMeta = lStfRdmaInfo.mStfMeta;
    const std::uint64_t lTfId = lStfMeta.stf_hdr_meta().stf_id();
    const std::string &lStfSenderId = lStfMeta.stf_sender_id();
    const auto &lTxgPtrs = lStfRdmaInfo.mTxgPtrs;

    auto lFmqPrepareStart = clock::now();

    DDMON("tfbuilder", "recv.rma_get_total_ms", lStfRdmaInfo.mRdmaTimeMs);
    DDMON_RATE("tfbuilder", "receive_time", (lStfRdmaInfo.mRdmaTimeMs / 1000.0));

    // signal in flight STF is finished (or error)
    mRpc->recordStfReceived(lStfSenderId, lTfId);

    // sort messages back to the original order
    std::sort(lStfMeta.mutable_stf_data_iov()->begin(), lStfMeta.mutable_stf_data_iov()->end(),
      [](auto &a, auto &b) { return a.idx() < b.idx(); });

    // create fmq messages from txgs
    lDataMsgsBuffers.clear();
    auto lDataVec = std::make_unique<std::vector<FairMQMessagePtr> >();
    lDataVec->reserve(lStfMeta.stf_data_iov_size());

    for (const auto &lDataMsg : lStfMeta.stf_data_iov()) {

      if ((lDataMsg.len() == 0) || (lDataMsg.start() == 0)) {
        lDataMsgsBuffers.emplace_back(nullptr, 0); // no payload message
      } else {
        assert (lDataMsg.txg() != std::uint32_t(-1));
        const auto &lTxg = lStfMeta.stf_txg_iov(lDataMsg.txg());
        const auto lTxgOff = (lDataMsg.start() - lTxg.start());

        assert (lDataMsg.start() >= lTxg.start());
        assert (lTxgOff <  lTxg.len());
        assert ((lTxgOff + lDataMsg.len()) <= lTxg.len());

        // store the address and length of a message within txg buffer
        lDataMsgsBuffers.emplace_back(reinterpret_cast<char*>(lTxgPtrs[lDataMsg.txg()])+lTxgOff, lDataMsg.len());
      }
    }

    // make data messages
    mTimeFrameBuilder.newDataFmqMessagesFromPtr(lDataMsgsBuffers, *lDataVec.get());

    // copy header meta
    auto lStfHdr = std::make_unique<IovStfHdrMeta>(std::move(lStfMeta.stf_hdr_meta()));

    DDMON("tfbuilder", "recv.fmq_msg_ms", since<std::chrono::milliseconds>(lFmqPrepareStart));

    const SubTimeFrame::Header lStfHeader = lStfReceiver.peek_tf_header(*lStfHdr.get());
    assert (lTfId == lStfHeader.mId);

    // send to deserializer thread so that we can keep receiving
    mReceivedDataQueue.push(lTfId, lStfHeader.mOrigin, lStfSenderId, std::move(lStfHdr), std::move(lDataVec));
  }

  DDDLOG("Exiting ucx postprocess thread {}", pThreadIdx);
}


} /* namespace o2::DataDistribution */
