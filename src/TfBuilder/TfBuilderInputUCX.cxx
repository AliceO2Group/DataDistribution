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

#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

static void client_ep_err_cb(void *arg, ucp_ep_h, ucs_status_t status)
{
  dd_ucx_conn_info *lConnInfo = reinterpret_cast<dd_ucx_conn_info*>(arg);
  if (lConnInfo) {
    lConnInfo->mInputUCX->handle_client_ep_error(lConnInfo, status);
  }
}

static void listen_conn_handle_cb(ucp_conn_request_h conn_request, void *arg)
{
  dd_ucp_listener_context_t *lCtx = reinterpret_cast<dd_ucp_listener_context_t*>(arg);
  lCtx->mInputUcx->new_conn_handle(conn_request);
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
  while(mState.load() == CONFIGURING) {
    // progress the listener worker
    const auto lProgress = ucp_worker_progress(listener_worker.ucp_worker);

    const auto lSleep = lProgress > 0 ? 1us : 5000us;

    auto lConnInfoOpt = mConnRequestQueue.pop_wait_for(lSleep);
    if (!lConnInfoOpt.has_value()) {
      continue;
    }

    // we have a connection request
    const auto lStfSenderAddr = std::get<0>(lConnInfoOpt.value());
    ucp_conn_request_h conn_request = std::get<2>(lConnInfoOpt.value());

    // Create stfsender (data) worker + endpoint
    auto lConnStruct = std::make_unique<dd_ucx_conn_info>(this);

    if (!ucx::util::create_ucp_worker(ucp_context, &lConnStruct->worker, lStfSenderAddr)) {
      continue;
    }
    // Create stfsender endpoint
    if (!ucx::util::create_ucp_ep(lConnStruct->worker.ucp_worker, conn_request, &lConnStruct->ucp_ep,
      client_ep_err_cb, lConnStruct.get(), lStfSenderAddr)) {
      continue;
    }

    // receive the StfSenderId
    auto lStfSenderIdOpt = ucx::io::ucx_receive_string(lConnStruct->worker);

    if (!lStfSenderIdOpt) {
      EDDLOG("ListenerThread: Connection request: Failed to receive StfSenderId");
      continue;
    }

    const auto &lStfSenderId = lStfSenderIdOpt.value();

    DDDLOG("UCXListenerThread::Connection request. stf_sender_id={}", lStfSenderId);
    lConnStruct->mStfSenderId = lStfSenderId;

    // add the connection info map
    std::scoped_lock lLock(mConnectionMapLock);
    assert (mConnMap.count(lStfSenderId) == 0);
    mConnMap[lStfSenderId] = std::move(lConnStruct);
  }

  DDDLOG("TfBuilderInputUCX: Listener thread stopped.");
}

bool TfBuilderInputUCX::start()
{
  // setting configuration options
  mThreadPoolSize = std::clamp(mConfig->getUInt64Param(UcxTfBuilderThreadPoolSizeKey, UcxTfBuilderThreadPoolSizeDefault), std::size_t(0), std::size_t(256));
  mThreadPoolSize = std::max(std::size_t(16), (mThreadPoolSize == 0) ? std::thread::hardware_concurrency() : mThreadPoolSize);
  mNumRmaOps = std::clamp(mConfig->getUInt64Param(UcxNumConcurrentRmaGetOpsKey, UcxNumConcurrentRmaGetOpsDefault), std::size_t(1), std::size_t(64));

  IDDLOG("TfBuilderInputUCX: Configuration loaded. thread_pool={} num_rma_ops={}", mThreadPoolSize, mNumRmaOps);

  auto &lConfStatus = mConfig->status();

  // disabled until the listener is initialized
  lConfStatus.mutable_ucx_info()->set_enabled(false);

  // Init and create UCX context
  if (!ucx::util::create_ucp_context(&ucp_context)) {
    return false;
  }

  // map the receive buffer for ucx rma
  {
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
  }

  // Create listener worker for accepting connections from StfSender
  if (!ucx::util::create_ucp_worker(ucp_context, &listener_worker, "Listener")) {
    return false;
  }

  // start receiving thread pool
  for (unsigned i = 0; i < mThreadPoolSize; i++) {
    std::string lThreadName = "tfb_ucx_";
    lThreadName += std::to_string(i);

    mThreadPool.emplace_back(std::move(create_thread_member(lThreadName.c_str(),
                                                            &TfBuilderInputUCX::DataHandlerThread, this, i)));
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
  std::uint32_t lNumStfSenders;
  if (!mRpc->TfSchedRpcCli().NumStfSendersInPartitionRequest(lNumStfSenders)) {
    WDDLOG_RL(5000, "gRPC error: cannot reach scheduler. scheduler_ep={}", mRpc->TfSchedRpcCli().getEndpoint());
    return false;
  }

  if (lNumStfSenders == 0 || lNumStfSenders == std::uint32_t(-1)) {
    EDDLOG("gRPC error: number of StfSenders in partition: {}." , lNumStfSenders);
    return false;
  }

  // Connect all StfSenders
  TfBuilderUCXConnectionResponse lConnResult;
  do {
    IDDLOG("Requesting StfSender connections from the TfScheduler.");

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
      std::scoped_lock lLock(mConnectionMapLock);
      lNumConnected = mConnMap.size();
    }

    if (lNumConnected == lNumStfSenders) {
      break;
    }

    std::this_thread::sleep_for(100ms);
    DDDLOG_RL(5000, "TfBuilderInputUCX::start: Waiting for all StfSender ucx endpoints. connected={} total={}", lNumConnected, lNumStfSenders);
  } while (true);

  // This will stop the Listener thread
  mState = RUNNING;

  DDDLOG("TfBuilderInputUCX::start: Finished");
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
  IDDLOG("TfBuilderInputUCX::stop: Listener thread stopped.");

  // Wait for input threads to stop
  DDDLOG("TfBuilderInputUCX::stop: Waiting for input threads to terminate.");
  mStfReqQueue.stop();
  for (auto& lIdThread : mThreadPool) {
    if (lIdThread.joinable())
      lIdThread.join();
  }
  mThreadPool.clear();
  DDDLOG("TfBuilderInputUCX::stop: All input threads terminated.");

  // Disconnect all input channels
  // RPC: Send disconnect request to scheduler
  {
    StatusResponse lResult;
    auto &lStatus = mConfig->status();
    if (mRpc->TfSchedRpcCli().TfBuilderUCXDisconnectionRequest(lStatus, lResult)) {
      DDDLOG("TfBuilderInputUCX::stop: RPC Request for StfSender disconnect successful.");
    } else {
      EDDLOG("TfBuilderInputUCX::stop: RPC error: Request for StfSender disconnect failed!");
    }
  }

  // unmap the receive buffers
  {
    // remove the ucx mapping from the region
    void* lOrigAddress = mTimeFrameBuilder.mMemRes.mDataMemRes->address();
    mTimeFrameBuilder.mMemRes.mDataMemRes->set_ucx_address(lOrigAddress);
    // unmap
    ucp_mem_unmap(ucp_context, ucp_data_region);
    DDDLOG("TfBuilderInputUCX::stop: RPC error: Request for StfSender disconnect failed!");
  }

  // close ucx: destroy remote rma keys and disconnect
  {
    std::scoped_lock lLock(mConnectionMapLock);

    for (auto & lConn : mConnMap) {
      std::scoped_lock lIoLock(lConn.second->mStfSenderIoLock);
      for (auto & lRKeyIt : lConn.second->mRemoteKeys) {
        ucp_rkey_destroy(lRKeyIt.second);
      }
      ucx::util::close_connection(lConn.second->worker, lConn.second->ucp_ep);
    }
    mConnMap.clear();
  }

  DDDLOG("TfBuilderInputUCX::stop: All input channels are closed.");
}

/// Receiving thread
void TfBuilderInputUCX::DataHandlerThread(const unsigned pThreadIdx)
{
  DDDLOG("Starting receiver thread[{}]", pThreadIdx);
  // Deserialization object (stf ID)
  IovDeserializer lStfReceiver(mTimeFrameBuilder);

  { // warm up FMQ region caches for this thread
    mTimeFrameBuilder.newDataMessage(1);
  }

  std::optional<std::string> lStfSenderIdOpt;
  std::vector<void*> lTxgPtrs;
  std::vector<std::pair<void*, std::size_t>> lDataMsgsBuffers;

  while ((lStfSenderIdOpt = mStfReqQueue.pop()) != std::nullopt) {
    using clock = std::chrono::steady_clock;

    const auto &lStfSenderId = lStfSenderIdOpt.value();
    std::uint64_t lTfId = 0;

    // Reference to the input channel
    dd_ucx_conn_info *lConn = nullptr;
    {
      std::scoped_lock lLock(mConnectionMapLock);
      if (mConnMap.count(lStfSenderId) == 1) {
        lConn = mConnMap.at(lStfSenderId).get();
        if (lConn && lConn->mConnError) {
          continue; // we are stoping anyway
        }
      } else {
        continue;
      }
    }

    UCXIovStfHeader lMeta;
    lTxgPtrs.clear();

    clock::time_point lMetaDecodeStart;
    {
      std::scoped_lock lStfSenderIoLock(lConn->mStfSenderIoLock);

      auto lStartLoop = clock::now();

      // Receive STF iov and metadata
      const auto lStfMetaDataOtp = ucx::io::ucx_receive_string(lConn->worker);

      if (!lStfMetaDataOtp.has_value()) {
        EDDLOG("DataHandlerThread {}: Failed to receive stf meta structure.", lStfSenderId);
        continue;
      }

      DDMON("tfbuilder", "recv.receive_meta_ms", since<std::chrono::milliseconds>(lStartLoop));
      lMetaDecodeStart = clock::now();

      const auto &lStfMetaData = lStfMetaDataOtp.value();

      lMeta.ParseFromString(lStfMetaData);

      lTfId = lMeta.stf_hdr_meta().stf_id();

      IDDLOG_GRL(1000, "Received StfMeta stf_id={} data_parts={}", lTfId, lMeta.stf_data_iov_size());

      // make sure we have remote keys unpacked
      for (const auto &lRegion : lMeta.data_regions() ) {
        if (lConn->mRemoteKeys.count(lRegion.region_rkey()) == 0) {
          DDDLOG("Mapping the new region size={}", lRegion.size());
          auto lNewRkeyIter = lConn->mRemoteKeys.emplace(lRegion.region_rkey(), ucp_rkey_h());
          ucp_ep_rkey_unpack(lConn->ucp_ep, lRegion.region_rkey().data(), &(lNewRkeyIter.first->second));
        }
      }

      DDMON("tfbuilder", "recv.meta_decode_ms", since<std::chrono::milliseconds>(lMetaDecodeStart));
      auto lAllocStart = clock::now();

      // Allocate data memory
      using UCXIovTxg = UCXIovStfHeader::UCXIovTxg;
      std::vector<std::uint64_t> lTxgSizes;

      lTxgSizes.reserve(lMeta.stf_txg_iov().size());
      lTxgPtrs.reserve(lMeta.stf_txg_iov().size());

      std::for_each(lMeta.stf_txg_iov().cbegin(), lMeta.stf_txg_iov().cend(), [&lTxgSizes](const UCXIovTxg &txg) {
        lTxgSizes.push_back(txg.len());
      });

      mTimeFrameBuilder.allocDataBuffers(lTxgSizes, lTxgPtrs);
      assert (!(lMeta.stf_txg_iov_size() > 0) || (lTxgPtrs.size() == (lMeta.stf_txg_iov().rbegin()->txg() + 1)));

      DDMON("tfbuilder", "recv.data_alloc_ms", since<std::chrono::milliseconds>(lAllocStart));

      // RMA get all the txgs
      auto lRmaGetStart = clock::now();
      ucx::io::dd_ucp_multi_req lRmaReqSem(mNumRmaOps);

      for (auto &lStfTxg : lMeta.stf_txg_iov()) {
        auto lTxgUcxPtr = mTimeFrameBuilder.mMemRes.mDataMemRes->get_ucx_ptr(lTxgPtrs[lStfTxg.txg()]);
        ucx::io::get(lConn->ucp_ep, lTxgUcxPtr, lStfTxg.len(), lStfTxg.start(), lConn->mRemoteKeys[lMeta.data_regions(lStfTxg.region()).region_rkey()], &lRmaReqSem);

        if (!ucx::io::ucp_wait(lConn->worker, lRmaReqSem)) {
          EDDLOG("Error from ucp_wait");
          break;
        }
      }
      // wait for final completion
      lRmaReqSem.mark_finished();
      if (!ucx::io::ucp_wait(lConn->worker, lRmaReqSem)) {
        EDDLOG("Error from ucp_wait");
        break;
      }

      // notify StfSender we completed
      std::string lOkStr = "OK";
      if (!ucx::io::ucx_send_string(lConn->worker, lConn->ucp_ep, lOkStr) ) {
        EDDLOG_GRL(10000, "StfSender was NOT notified about transfer finish stf_sender={} tf_id={}", lStfSenderId, lTfId);
      }

      DDMON("tfbuilder", "recv.rma_get_total_ms", since<std::chrono::milliseconds>(lRmaGetStart));
    }

    auto lFmqPrepareStart = clock::now();

    // signal in flight STF is finished (or error)
    mRpc->recordStfReceived(lStfSenderId, lTfId);

    // sort messages back to the original order
    std::sort(lMeta.mutable_stf_data_iov()->begin(), lMeta.mutable_stf_data_iov()->end(), [](auto &a, auto &b) { return a.idx() < b.idx(); });

    // create fmq messages from txgs
    lDataMsgsBuffers.clear();
    auto lDataVec = std::make_unique<std::vector<FairMQMessagePtr> >();
    lDataVec->reserve(lMeta.stf_data_iov_size());

    for (const auto &lDataMsg : lMeta.stf_data_iov()) {
      const auto &lTxg = lMeta.stf_txg_iov(lDataMsg.txg());

      assert (lDataMsg.start() >= lTxg.start());
      const auto lTxgOff = (lDataMsg.start() - lTxg.start());

      assert (lTxgOff <  lTxg.len());
      assert ((lTxgOff + lDataMsg.len()) <= lTxg.len());

      lDataMsgsBuffers.emplace_back(reinterpret_cast<char*>(lTxgPtrs[lDataMsg.txg()])+lTxgOff, lDataMsg.len());
    }

    // make data messages
    mTimeFrameBuilder.newDataFmqMessagesFromPtr(lDataMsgsBuffers, *lDataVec.get());

    // copy header meta
    auto lStfHdr = std::make_unique<IovStfHdrMeta>(std::move(lMeta.stf_hdr_meta()));

    DDMON("tfbuilder", "recv.fmq_msg_ms", since<std::chrono::milliseconds>(lFmqPrepareStart));
    DDMON("tfbuilder", "recv.total_ms", since<std::chrono::milliseconds>(lMetaDecodeStart));

    const SubTimeFrame::Header lStfHeader = lStfReceiver.peek_tf_header(*lStfHdr.get());
    assert (lTfId == lStfHeader.mId);

    // send to deserializer thread so that we can keep receiving
    mReceivedDataQueue.push(lTfId, lStfHeader.mOrigin, lStfSenderId, std::move(lStfHdr), std::move(lDataVec));
  }

  DDDLOG("Exiting UCX input thread[{}]", pThreadIdx);
}


} /* namespace o2::DataDistribution */
