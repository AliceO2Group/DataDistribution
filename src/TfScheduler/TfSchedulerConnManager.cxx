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
#include <future>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

std::size_t TfSchedulerConnManager::checkStfSenders()
{
  // TODO: monitor and reconnect?
  return mStfSenderRpcClients.size();
}

void TfSchedulerConnManager::connectTfBuilder(const TfBuilderConfigStatus &pTfBuilderStatus, TfBuilderConnectionResponse &pResponse /*out*/)
{
  pResponse.Clear();

  const std::string &lTfBuilderId = pTfBuilderStatus.info().process_id();

  if (pTfBuilderStatus.sockets().map().size() != mPartitionInfo.mStfSenderIdList.size()) {
    DDLOGF(fair::Severity::ERROR, "TfBuilder Connection error: Number of open sockets doesn't match the number of StfSenders. num_sockets={} num_stfs={}",
      pTfBuilderStatus.sockets().map().size(), mPartitionInfo.mStfSenderIdList.size());
    pResponse.set_status(ERROR_SOCKET_COUNT);
    return;
  }

  std::scoped_lock lLock(mStfSenderClientsLock);

  if (!stfSendersReady()) {
    DDLOGF(fair::Severity::INFO, "TfBuilder Connection error: StfSenders not ready.");
    pResponse.set_status(ERROR_STF_SENDERS_NOT_READY);
    return;
  }

  // Open the gRPC connection to the new TfBuilder
  if (!newTfBuilderRpcClient(lTfBuilderId)) {
    DDLOGF(fair::Severity::WARNING, "TfBuilder gRPC connection error: Cannot open the gRPC connection. tfb_id={}",
      lTfBuilderId);
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
      DDLOGF(fair::Severity::ERROR, "TfBuilder Connection error: gRPC error when connecting StfSender. stfs_id={} tfb_id={}",
        lStfSenderId, lTfBuilderId);
      pResponse.set_status(ERROR_GRPC_STF_SENDER);
      lConnectionsOk = false;
      break;
    }

    // check StfSender status
    if (lResponse.status() != OK) {
      DDLOGF(fair::Severity::ERROR, "TfBuilder Connection error: cannot connect. stfs_id={} tfb_id={}",
        lStfSenderId, lTfBuilderId);
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

  std::scoped_lock lLock(mStfSenderClientsLock);

  deleteTfBuilderRpcClient(lTfBuilderId);

  if (!stfSendersReady()) {
    DDLOGF(fair::Severity::INFO, "TfBuilder Connection error: StfSenders not ready.");
    pResponse.set_status(ERROR_STF_SENDERS_NOT_READY);
    return;
  }

  TfBuilderEndpoint lParam;

  for (const auto &[lTfBuilderSocketIdx, lSocketInfo] : pTfBuilderStatus.sockets().map()) {
    (void) lTfBuilderSocketIdx;

    const auto &lStfSenderId = lSocketInfo.peer_id();

    if (lStfSenderId.empty()) {
      continue; // not connected
    }

    lParam.set_tf_builder_id(lTfBuilderId);
    lParam.set_endpoint(lSocketInfo.endpoint());

    if (mStfSenderRpcClients.count(lStfSenderId) == 0) {
      DDLOGF(fair::Severity::WARN, "disconnectTfBuilder: Unknown StfSender. stfs_id={}", lStfSenderId);
      continue;
    }

    auto &lRpcClient = mStfSenderRpcClients[lSocketInfo.peer_id()];

    StatusResponse lResponse;
    if(!lRpcClient->DisconnectTfBuilderRequest(lParam, lResponse).ok()) {
      DDLOGF(fair::Severity::ERROR, "TfBuilder Connection error: gRPC error. stfs_id={} tfb_id={}",
        lStfSenderId, lTfBuilderId);
      pResponse.set_status(ERROR_GRPC_STF_SENDER);
      break;
    }

    // check StfSender status
    if (lResponse.status() != 0) {
      DDLOGF(fair::Severity::ERROR, "TfBuilder Connection error. stfs_id={} tfb_id={} response={}",
        lStfSenderId, lTfBuilderId, lResponse.status());
      pResponse.set_status(ERROR_STF_SENDER_CONNECTING);
      break;
    }
  }
}


void TfSchedulerConnManager::removeTfBuilder(const std::string &pTfBuilderId)
{
  std::scoped_lock lLock(mStfSenderClientsLock);

  // Stop talking to TfBuilder
  deleteTfBuilderRpcClient(pTfBuilderId);

  DDLOGF(fair::Severity::DEBUG, "TfBuilder RpcClient deleted. tfb_id={}", pTfBuilderId);

  // Tell all StfSenders to disconnect
  TfBuilderEndpoint lParam;
  lParam.set_tf_builder_id(pTfBuilderId);

  for (auto &lStfSenderIdCli : mStfSenderRpcClients) {
    const auto &lStfSenderId = lStfSenderIdCli.first;
    auto &lStfSenderRpcCli = lStfSenderIdCli.second;

    StatusResponse lResponse;
    if(!lStfSenderRpcCli->DisconnectTfBuilderRequest(lParam, lResponse).ok()) {
      DDLOGF(fair::Severity::ERROR, "TfBuilder Connection error: gRPC error when connecting StfSender. stfs_id={} tfb_id={}",
        lStfSenderId, pTfBuilderId);
    }

    // check StfSender status
    if (lResponse.status() != 0) {
      DDLOGF(fair::Severity::ERROR, "DisconnectTfBuilderRequest failed. stfs_id={} tfb_id={} response={}",
        lStfSenderId, pTfBuilderId, lResponse.status());
    }
  }
}

void TfSchedulerConnManager::dropAllStfsAsync(const std::uint64_t pStfId)
{

  auto lDropLambda = [&](const std::uint64_t pLamStfId) -> std::uint64_t {
    StfDataRequestMessage lStfRequest;
    StfDataResponse lStfResponse;
    lStfRequest.set_tf_builder_id("-1");
    lStfRequest.set_stf_id(pLamStfId);

    for (auto &lStfSenderIdCli : mStfSenderRpcClients) {
      const auto &lStfSenderId = lStfSenderIdCli.first;
      auto &lStfSenderRpcCli = lStfSenderIdCli.second;

      auto lStatus = lStfSenderRpcCli->StfDataRequest(lStfRequest, lStfResponse);
      if (!lStatus.ok()) {
        // gRPC problem... continue asking for other
        DDLOGF(fair::Severity::WARNING, "StfSender gRPC connection error. stfs_id={} code={} error={}",
          lStfSenderId, lStatus.error_code(), lStatus.error_message());
      }

      if (lStfResponse.status() == StfDataResponse::DATA_DROPPED_TIMEOUT) {
        DDLOGF(fair::Severity::WARNING, "StfSender dropped an STF before notification from the TfScheduler. "
          "Check the StfSender buffer state. stfs_id={} stf_id={}",
          lStfSenderId, pLamStfId);
      } else if (lStfResponse.status() == StfDataResponse::DATA_DROPPED_UNKNOWN) {
        DDLOGF(fair::Severity::WARNING, "StfSender dropped an STF for unknown reason. "
          "Check the StfSender buffer state. stfs_id={} stf_id={}",
          lStfSenderId, pLamStfId);
      }
    }

    return pLamStfId;
  };

  try {
    auto lFeature = std::async(std::launch::async, lDropLambda, pStfId);

    std::scoped_lock lLock(mStfDropFuturesLock);
    mStfDropFutures.emplace_back(std::move(lFeature));

  } catch (std::exception &) {
    DDLOGF(fair::Severity::WARNING, "dropAllStfsAsync: async method failed. Calling synchronously.");
    lDropLambda(pStfId);
  }

}

void TfSchedulerConnManager::StfSenderMonitoringThread()
{
  DDLOGF(fair::Severity::DEBUG, "Starting StfSender gRPC Monitoring thread.");
  std::uint64_t lDroppedTotal = 0;

  std::vector<std::uint64_t> lDroppedStfs;

  while (mRunning) {
    // make sure all StfSenders are alive
    const std::uint32_t lNumStfSenders = checkStfSenders();
    if (lNumStfSenders < mPartitionInfo.mStfSenderIdList.size()) {
      DDLOGF(fair::Severity::INFO, "Waiting for StfSenders. connected={} total={}",
        lNumStfSenders, mPartitionInfo.mStfSenderIdList.size() );
      std::this_thread::sleep_for(1000ms);
      continue;
    }

    // wait for drop futures
    {
      {
        std::scoped_lock lLock(mStfDropFuturesLock);
        for (auto lFutureIt = mStfDropFutures.begin(); lFutureIt != mStfDropFutures.end(); lFutureIt++) {
          if (std::future_status::ready == lFutureIt->wait_for(std::chrono::seconds(0))) {
            assert (lFutureIt->valid());
            lDroppedStfs.push_back(lFutureIt->get());
            lFutureIt = mStfDropFutures.erase(lFutureIt);
          }
        }
      }

      sort(lDroppedStfs.begin(), lDroppedStfs.end());
      for (auto &lDroppedId : lDroppedStfs) {
        lDroppedTotal++;
        DDLOGF_RL(1000, fair::Severity::INFO, "Dropped SubTimeFrame (cannot schedule). stf_id={} total={}",
          lDroppedId, lDroppedTotal);
      }
      lDroppedStfs.clear();
    }

    std::this_thread::sleep_for(1000ms);
  }

  DDLOGF(fair::Severity::DEBUG, "Exiting StfSender RPC Monitoring thread.");
}


}
} /* o2::DataDistribution */
