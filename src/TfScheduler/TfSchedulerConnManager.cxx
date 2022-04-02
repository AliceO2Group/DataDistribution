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

#include "TfSchedulerConnManager.h"
#include "TfSchedulerInstanceRpc.h"

#include <StfSenderRpcClient.h>

#include <set>
#include <tuple>
#include <algorithm>
#include <shared_mutex>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

bool TfSchedulerConnManager::start()
{
  using namespace std::chrono_literals;

  while (!mStfSenderRpcClients.start()) {
    return false; // we'll be called back
  }

  // start stf drop threads
  for (int i = 0; i < 8; i++) {
    std::string lThreadName = "stf_drop_" + std::to_string(i);
    mStfDropThreads.emplace_back(create_thread_member(lThreadName.c_str(),
      &TfSchedulerConnManager::DropStfThread, this));
  }

  // start all Connection threads
  {
    std::unique_lock lRpcLock(mStfSenderClientsLock);
    std::unique_lock lConnInfoLock(mConnectInfoLock);

    for (auto &[lStfSenderId, lRpcClient] : mStfSenderRpcClients) {

      mConnectThreadInfos.emplace(std::make_pair(lStfSenderId, StfSenderUCXThreadInfo()));

      std::string lThreadName = "ucx_conn_" + lStfSenderId;
      mConnectionThreads.push_back(create_thread_member(lThreadName.c_str(),
        &TfSchedulerConnManager::ConnectTfBuilderUCXThread, this, lStfSenderId));
    }
  }

  mRunning = true;

  // start gRPC client monitoring thread
  mStfSenderMonitoringThread = create_thread_member("sched_stfs_mon",
    &TfSchedulerConnManager::StfSenderMonitoringThread, this);

  IDDLOG("TfSchedulerConnManager::start() done");
  return true;
}

void TfSchedulerConnManager::stop()
{
  DDDLOG("TfSchedulerConnManager::stop()");

  mRunning = false;

  if (mStfSenderMonitoringThread.joinable()) {
    mStfSenderMonitoringThread.join();
  }

  // stop stf drop threads
  {
    mStfDropQueue.stop();
    for (auto &lThread : mStfDropThreads) {
      if (lThread.joinable()) {
        lThread.join();
      }
    }
  }

  // start all Connection threads
  {
    std::unique_lock lConnInfoLock(mConnectInfoLock);
    for (auto &[lStfSenderId, lThreadInfo] : mConnectThreadInfos) {
      lThreadInfo.mConnReqQueue->stop();
    }

    for (auto &lConnThread : mConnectionThreads) {
      if (lConnThread.joinable()) {
        lConnThread.join();
      }
    }
  }
  // delete all rpc clients
  mStfSenderRpcClients.stop();

  IDDLOG("TfSchedulerConnManager::top() done");
}

std::size_t TfSchedulerConnManager::checkStfSenders()
{
  std::size_t lReadyCnt = 0;
  for (const auto &lId : mPartitionInfo.mStfSenderIdList) {
    // this will attempt reconnection on existing connections
    if (checkStfSenderRpcConn(lId)) {
      lReadyCnt++;
    }
  }

  return lReadyCnt;
}

void TfSchedulerConnManager::connectTfBuilder(const TfBuilderConfigStatus &pTfBuilderStatus, TfBuilderConnectionResponse &pResponse /*out*/)
{
  pResponse.Clear();

  if (!pTfBuilderStatus.sockets().enabled()) {
    EDDLOG("TfBuilder FairMQ Connection error: TfBuilder does not have UCX listener enabled.");
    pResponse.set_status(ERROR_TRANSPORT_NOT_ENABLED);
    return;
  }

  const std::string &lTfBuilderId = pTfBuilderStatus.info().process_id();

  if (pTfBuilderStatus.sockets().map().size() != mPartitionInfo.mStfSenderIdList.size()) {
    EDDLOG("TfBuilder Connection error: Number of open sockets doesn't match the number of StfSenders. num_sockets={} num_stfs={}",
      pTfBuilderStatus.sockets().map().size(), mPartitionInfo.mStfSenderIdList.size());
    pResponse.set_status(ERROR_SOCKET_COUNT);
    return;
  }

  std::scoped_lock lLock(mStfSenderClientsLock);

  if (!mRunning || !stfSendersReady()) {
    IDDLOG("TfBuilder Connection error: StfSenders not ready.");
    pResponse.set_status(ERROR_STF_SENDERS_NOT_READY);
    return;
  }

  // Open the gRPC connection to the new TfBuilder
  if (!newTfBuilderRpcClient(lTfBuilderId)) {
    WDDLOG("TfBuilder gRPC connection error: Cannot open the gRPC connection. tfb_id={}", lTfBuilderId);
    pResponse.set_status(ERROR_GRPC_TF_BUILDER);
    return;
  }

  // send message to all StfSenders to connect
  bool lConnectionsOk = true;
  pResponse.set_status(OK);

  TfBuilderEndpoint lParam;
  lParam.set_tf_builder_id(lTfBuilderId);
  std::uint32_t lEndpointIdx = 0;
  for (auto &[lStfSenderId, lRpcClient] : mStfSenderRpcClients) {

    lParam.set_endpoint(pTfBuilderStatus.sockets().map().at(lEndpointIdx).endpoint());

    ConnectTfBuilderResponse lResponse;
    if(!lRpcClient->ConnectTfBuilderRequest(lParam, lResponse).ok()) {
      EDDLOG_RL(1000, "TfBuilder Connection error: gRPC error when connecting StfSender. stfs_id={} tfb_id={}",
        lStfSenderId, lTfBuilderId);
      pResponse.set_status(ERROR_GRPC_STF_SENDER);
      lConnectionsOk = false;
      break;
    }

    // check StfSender status
    if (lResponse.status() != OK) {
      EDDLOG_RL(1000, "TfBuilder Connection error: cannot connect. stfs_id={} tfb_id={}", lStfSenderId, lTfBuilderId);
      pResponse.set_status(lResponse.status());
      lConnectionsOk = false;
      break;
    }

    // save connection for response
    auto &lConnMap = *(pResponse.mutable_connection_map());
    lConnMap[lEndpointIdx] = lStfSenderId;

    lEndpointIdx++;
  }

  if (! lConnectionsOk) {
    StatusResponse lResponse;
    // remove all existing connection
    disconnectTfBuilder(pTfBuilderStatus, lResponse);
  }
}

void TfSchedulerConnManager::disconnectTfBuilder(const TfBuilderConfigStatus &pTfBuilderStatus, StatusResponse &pResponse /*out*/)
{
  pResponse.set_status(0);
  const std::string &lTfBuilderId = pTfBuilderStatus.info().process_id();

  {
    std::scoped_lock lLock(mStfSenderClientsLock);
    deleteTfBuilderRpcClient(lTfBuilderId);
  }

  TfBuilderEndpoint lParam;

  for (const auto &[lTfBuilderSocketIdx, lSocketInfo] : pTfBuilderStatus.sockets().map()) {
    (void) lTfBuilderSocketIdx;

    const auto &lStfSenderId = lSocketInfo.peer_id();

    if (lStfSenderId.empty()) {
      continue; // not connected
    }

    { // lock clients
      std::shared_lock lLock(mStfSenderClientsLock);

      if (mStfSenderRpcClients.count(lStfSenderId) == 0) {
        WDDLOG("disconnectTfBuilder: Unknown StfSender. stfs_id={}", lStfSenderId);
        continue;
      }

      lParam.set_tf_builder_id(lTfBuilderId);
      lParam.set_endpoint(lSocketInfo.endpoint());
      StatusResponse lResponse;

      auto &lRpcClient = mStfSenderRpcClients[lSocketInfo.peer_id()];
      if(!lRpcClient->DisconnectTfBuilderRequest(lParam, lResponse).ok()) {
        IDDLOG_RL(1000, "StfSender disconnection error: gRPC error. stfs_id={} tfb_id={}", lStfSenderId, lTfBuilderId);
        pResponse.set_status(ERROR_GRPC_STF_SENDER);
        continue;
      }
      // check StfSender status
      if (lResponse.status() != 0) {
        IDDLOG_RL(1000, "TfBuilder disconnection error. stfs_id={} tfb_id={} response={}", lStfSenderId, lTfBuilderId, lResponse.status());
        pResponse.set_status(ERROR_STF_SENDER_CONNECTING);
        continue;
      }
    }
  }
}

void TfSchedulerConnManager::connectTfBuilderUCX(const TfBuilderConfigStatus &pTfBuilderStatus, TfBuilderUCXConnectionResponse &pResponse /*out*/)
{
  pResponse.Clear();

  if (!pTfBuilderStatus.ucx_info().enabled()) {
    EDDLOG("TfBuilder UCX Connection error: TfBuilder does not have UCX listener enabled.");
    pResponse.set_status(ERROR_TRANSPORT_NOT_ENABLED);
    return;
  }

  const std::string &lTfBuilderId = pTfBuilderStatus.info().process_id();

  std::shared_lock lLock(mStfSenderClientsLock);

  if (!mRunning || !stfSendersReady()) {
    IDDLOG("TfBuilder UCX Connection: StfSenders gRPC connection not ready.");
    pResponse.set_status(ERROR_STF_SENDERS_NOT_READY);
    return;
  }

  // send message to all StfSenders to connect
  bool lConnectionsOk = true;
  pResponse.set_status(OK);

  TfBuilderUCXEndpoint lParam;
  lParam.set_tf_builder_id(lTfBuilderId);
  lParam.mutable_endpoint()->CopyFrom(pTfBuilderStatus.ucx_info());

  DDDLOG("connectTfBuilderUCX: starting connections for tf_builder_id={}", lTfBuilderId);

  ConcurrentQueue<std::tuple<bool, std::string, ConnectTfBuilderUCXResponse>> lConnRepQueue;
  std::size_t lStfSenderCnt = 0;
  // queue all connection requests
  {
    std::shared_lock lStfSenderInfoLock(mConnectInfoLock);
    lStfSenderCnt = mConnectThreadInfos.size();
    for (auto &[lStfSenderId, lThreadInfo] : mConnectThreadInfos) {
      (void)lStfSenderId;
      auto &lReqQueue = lThreadInfo.mConnReqQueue;
      lReqQueue->push(std::make_unique<StfSenderUCXConnectReq>(lParam, &lConnRepQueue));
    }
  }

  DDDLOG("connectTfBuilderUCX: starting gRPC client for tf_builder_id={}", lTfBuilderId);

  // Open the gRPC connection to the new TfBuilder (will only add if already does not exist)
  if (!newTfBuilderRpcClient(lTfBuilderId)) {
    WDDLOG("TfBuilder gRPC connection error: Cannot open the gRPC connection. tfb_id={}", lTfBuilderId);
    pResponse.set_status(ERROR_GRPC_TF_BUILDER);
    return;
  }

  // wait for all connection replies
  for (std::size_t i = 0; i < lStfSenderCnt; i++) {

    std::tuple<bool, std::string, ConnectTfBuilderUCXResponse> lRep;
    bool lRepOk = lConnRepQueue.pop(lRep);

    if (!lRepOk && !std::get<0>(lRep)) {
      pResponse.set_status(ERROR_GRPC_STF_SENDER);
      lConnectionsOk = false;
      break;
    }

    const auto &lStfSenderId = std::get<1>(lRep);
    auto &lResponse = std::get<2>(lRep);

    // check StfSender status
    if (lResponse.status() != OK) {
      pResponse.set_status(lResponse.status());
      lConnectionsOk = false;
      break;
    }

    // save connection for response
    auto lConnMap = pResponse.mutable_connection_map();
    lConnMap->insert({ lStfSenderId, lResponse.stf_sender_ep()});
  }

  if (! lConnectionsOk) {
    StatusResponse lResponse;
    // remove all existing connection
    disconnectTfBuilderUCX(pTfBuilderStatus, lResponse);
    assert (pResponse.status() != OK);
  }

  DDDLOG("connectTfBuilderUCX: finished for tf_builder_id={} success={}", lTfBuilderId, lConnectionsOk);
}

// per FLP thread to connect TfBuilders
void TfSchedulerConnManager::ConnectTfBuilderUCXThread(const std::string lStfSenderId)
{
  // Initialize the thread queue
  std::unique_lock lThreadLock(mConnectInfoLock);
  auto &lConnReqQueue = mConnectThreadInfos[lStfSenderId].mConnReqQueue;

  lThreadLock.unlock();

  std::optional<std::unique_ptr<StfSenderUCXConnectReq>> lStfSenderIdOpt;

  DDDLOG("ConnectTfBuilderUCXThread started for stf_sender_id={}", lStfSenderId);

  while ((lStfSenderIdOpt = lConnReqQueue->pop()) != std::nullopt) {

    bool lConnectionsOk = true;

    auto &lRpcClient = mStfSenderRpcClients[lStfSenderId];
    const auto &lParam = lStfSenderIdOpt.value()->mRpcReq;
    const auto &lTfBuilderId = lParam.tf_builder_id();

    ConnectTfBuilderUCXResponse lResponse;
    if(!lRpcClient->ConnectTfBuilderUCXRequest(lParam, lResponse).ok()) {
      EDDLOG_RL(1000, "TfBuilder UCX Connection error: gRPC error when connecting StfSender. stfs_id={} tfb_id={}",
        lStfSenderId, lTfBuilderId);
      lConnectionsOk = false;
      break;
    }

    // check StfSender status
    if (lResponse.status() != OK) {
      EDDLOG_RL(1000, "TfBuilder UCX Connection error: cannot connect. stfs_id={} tfb_id={}", lStfSenderId, lTfBuilderId);
      lConnectionsOk = false;
      break;
    }

    // send reply
    auto mConnRepQueue = lStfSenderIdOpt.value()->mConnRepQueue;

    if (mConnRepQueue) {
      mConnRepQueue->push(std::make_tuple(lConnectionsOk, lStfSenderId, std::move(lResponse)));
    }
  }
}

void TfSchedulerConnManager::disconnectTfBuilderUCX(const TfBuilderConfigStatus &pTfBuilderStatus, StatusResponse &pResponse /*out*/)
{
  pResponse.set_status(0);
  const std::string &lTfBuilderId = pTfBuilderStatus.info().process_id();

  std::scoped_lock lLock(mStfSenderClientsLock);
  // Remove TfBuilder RPC client (it might be removed already)
  deleteTfBuilderRpcClient(lTfBuilderId);

  // Send disconnect to all StfSender peers
  TfBuilderUCXEndpoint lParam;
  lParam.set_tf_builder_id(lTfBuilderId);
  lParam.mutable_endpoint()->CopyFrom(pTfBuilderStatus.ucx_info());

  for (auto &[lStfSenderId, lRpcClient] : mStfSenderRpcClients) {
    StatusResponse lResponse;
    if(!lRpcClient->DisconnectTfBuilderUCXRequest(lParam, lResponse).ok()) {
      EDDLOG_RL(1000, "TfBuilder UCX Disconnection error: gRPC error when connecting StfSender. stfs_id={} tfb_id={}",
        lStfSenderId, lTfBuilderId);
      pResponse.set_status(ERROR_GRPC_STF_SENDER);
      continue;
    }

    // check StfSender status
    if (lResponse.status() != 0) {
      IDDLOG_RL(1000, "TfBuilder disconnection error. stfs_id={} tfb_id={} response={}", lStfSenderId, lTfBuilderId, lResponse.status());
      pResponse.set_status(ERROR_STF_SENDER_CONNECTING);
      continue;
    }
  }
}


// Partition RPC: keep sending until all TfBuilders are gone
bool TfSchedulerConnManager::requestTfBuildersTerminate() {
  std::vector<std::string> lFailedRpcsForDeletion;

  std::scoped_lock lLock(mStfSenderClientsLock);

  for (auto &lTfBuilder : mTfBuilderRpcClients) {
    if (!lTfBuilder.second.mClient->TerminatePartition()) {
      lFailedRpcsForDeletion.push_back(lTfBuilder.first);
    }
  }

  for (const auto &lId : lFailedRpcsForDeletion) {
    deleteTfBuilderRpcClient(lId);
  }

  return mTfBuilderRpcClients.size() == 0;
}

// Partition RPC: notify all StfSenders and remove rpc clients
bool TfSchedulerConnManager::requestStfSendersTerminate() {
  std::vector<std::string> lFailedRpcsForDeletion;

  std::scoped_lock lLock(mStfSenderClientsLock);

  for (auto &lStfSender : mStfSenderRpcClients) {
    if (!lStfSender.second->TerminatePartition()) {
      lFailedRpcsForDeletion.push_back(lStfSender.first);
    }
  }

  for (const auto &lId : lFailedRpcsForDeletion) {
    deleteTfBuilderRpcClient(lId);
  }

  return mTfBuilderRpcClients.size() == 0;
}


void TfSchedulerConnManager::removeTfBuilder(const std::string &pTfBuilderId)
{
  std::scoped_lock lLock(mStfSenderClientsLock);

  // Stop talking to TfBuilder
  deleteTfBuilderRpcClient(pTfBuilderId);

  DDDLOG("TfBuilder RpcClient deleted. tfb_id={}", pTfBuilderId);

  // Tell all StfSenders to disconnect
  TfBuilderEndpoint lParam;
  lParam.set_tf_builder_id(pTfBuilderId);

  for (auto &lStfSenderIdCli : mStfSenderRpcClients) {
    const auto &lStfSenderId = lStfSenderIdCli.first;
    auto &lStfSenderRpcCli = lStfSenderIdCli.second;

    StatusResponse lResponse;
    if(!lStfSenderRpcCli->DisconnectTfBuilderRequest(lParam, lResponse).ok()) {
      EDDLOG("TfBuilder Connection error: gRPC error when connecting StfSender. stfs_id={} tfb_id={}",
        lStfSenderId, pTfBuilderId);
    }

    // check StfSender status
    if (lResponse.status() != 0) {
      EDDLOG("DisconnectTfBuilderRequest failed. stfs_id={} tfb_id={} response={}",
        lStfSenderId, pTfBuilderId, lResponse.status());
    }
  }
}

void TfSchedulerConnManager::dropSingleStfsAsync(const std::uint64_t pStfId, const std::string &pStfSenderId)
{
  mStfDropQueue.push(std::make_pair(pStfSenderId, pStfId));
}

void TfSchedulerConnManager::dropAllStfsAsync(const std::uint64_t pStfId)
{
  std::shared_lock lRpcLock(mStfSenderClientsLock);
  for (auto &lStfSenderIdCli : mStfSenderRpcClients) {
    const auto &lStfSenderId = lStfSenderIdCli.first;
    mStfDropQueue.push(std::make_pair(lStfSenderId, pStfId));
  }
}


void TfSchedulerConnManager::DropStfThread()
{
  DDDLOG("Starting DropThread thread.");
  std::optional<std::pair<std::string, std::uint64_t>> lDropReqOpt;

  while((lDropReqOpt = mStfDropQueue.pop()) != std::nullopt) {
    StfDataRequestMessage lStfRequest;
    StfDataResponse lStfResponse;

    const auto &lStfSenderId = lDropReqOpt.value().first;
    const auto lStfId = lDropReqOpt.value().second;

    lStfRequest.set_tf_builder_id("-1");
    lStfRequest.set_stf_id(lStfId);

    std::shared_lock lRpcLock(mStfSenderClientsLock);
    if (mStfSenderRpcClients.count(lStfSenderId) == 0) {
      EDDLOG_RL(10000, "DropStfThread: Unknown StfSender. stfs_id={}", lStfSenderId);
      continue;
    }

    auto &lStfSenderRpcCli = mStfSenderRpcClients[lStfSenderId];

    auto lStatus = lStfSenderRpcCli->StfDataDropRequest(lStfRequest, lStfResponse);
    if (!lStatus.ok()) {
      // gRPC problem...
      WDDLOG_GRL(1000, "StfSender gRPC connection error. stfs_id={} code={} error={}",
        lStfSenderId, lStatus.error_code(), lStatus.error_message());
    }

    if (lStfResponse.status() == StfDataResponse::DATA_DROPPED_TIMEOUT) {
      WDDLOG_GRL(1000, "StfSender dropped an STF before notification from the TfScheduler. "
        "Check the StfSender buffer state. stfs_id={} stf_id={}", lStfSenderId, lStfId);
    } else if (lStfResponse.status() == StfDataResponse::DATA_DROPPED_UNKNOWN) {
      WDDLOG_GRL(1000, "StfSender dropped an STF for unknown reason. "
        "Check the StfSender buffer state. stfs_id={} stf_id={}", lStfSenderId, lStfId);
    }
  }
}

void TfSchedulerConnManager::StfSenderMonitoringThread()
{
  DDDLOG("Starting StfSender gRPC Monitoring thread.");

  while (mRunning) {
    std::chrono::milliseconds lSleep = 1000ms;

    // make sure all StfSenders are alive
    const std::uint32_t lNumStfSenders = checkStfSenders();
    if (lNumStfSenders < mPartitionInfo.mStfSenderIdList.size()) {

      mStfSenderState = STF_SENDER_STATE_INCOMPLETE;

      WDDLOG_RL(1000, "Waiting for StfSenders. ready={} total={}", lNumStfSenders, mPartitionInfo.mStfSenderIdList.size());
      lSleep = 250ms;
    } else {
      mStfSenderState = STF_SENDER_STATE_OK;
    }

    std::this_thread::sleep_for(lSleep);
  }

  DDDLOG("Exiting StfSender RPC Monitoring thread.");
}

} /* o2::DataDistribution */
