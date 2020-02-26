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
    DDLOG(fair::Severity::ERROR) << "TfBuilder Connection error: Number of open sockets doesn't match number of StfSenders: " <<
      pTfBuilderStatus.sockets().map().size() << " != " << mPartitionInfo.mStfSenderIdList.size();
    pResponse.set_status(ERROR_SOCKET_COUNT);
    return;
  }

  std::scoped_lock lLock(mStfSenderClientsLock);

  if (!stfSendersReady()) {
    DDLOG(fair::Severity::DEBUG) << "TfBuilder Connection error: StfSenders not ready.";
    pResponse.set_status(ERROR_STF_SENDERS_NOT_READY);
    return;
  }

  // Open the gRPC connection to the new TfBuilder
  if (!newTfBuilderRpcClient(lTfBuilderId)) {
    DDLOG(fair::Severity::WARNING) << "TfBuilder gRPC connection error: Cannot open the gRPC connection to the TfBuilder: " << lTfBuilderId;
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
      DDLOG(fair::Severity::ERROR) << "TfBuilder Connection error: gRPC error when connecting StfSender " << lStfSenderId << " to " << lTfBuilderId;
      pResponse.set_status(ERROR_GRPC_STF_SENDER);
      lConnectionsOk = false;
      break;
    }

    // check StfSender status
    if (lResponse.status() != OK) {
      DDLOG(fair::Severity::ERROR) << "TfBuilder Connection error: StfSender " << lStfSenderId << " could not connect to " << lTfBuilderId;
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
    DDLOG(fair::Severity::DEBUG) << "TfBuilder Connection error: StfSenders not ready.";
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
      DDLOG(fair::Severity::WARN) << "disconnectTfBuilder: Unknown StfSender Id: " << lStfSenderId;
      continue;
    }

    auto &lRpcClient = mStfSenderRpcClients[lSocketInfo.peer_id()];

    StatusResponse lResponse;
    if(!lRpcClient->DisconnectTfBuilderRequest(lParam, lResponse).ok()) {
      DDLOG(fair::Severity::ERROR) << "TfBuilder Connection error: gRPC error when connecting StfSender " << lStfSenderId << " to " << lTfBuilderId;
      pResponse.set_status(ERROR_GRPC_STF_SENDER);
      break;
    }

    // check StfSender status
    if (lResponse.status() != 0) {
      DDLOG(fair::Severity::ERROR) << "TfBuilder Connection error: StfSender " << lStfSenderId << " could not connect to " << lTfBuilderId;
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

  DDLOG(fair::Severity::DEBUG) << "TfBuilder RpcClient deleted, id: " << pTfBuilderId;

  // Tell all StfSenders to disconnect
  TfBuilderEndpoint lParam;
  lParam.set_tf_builder_id(pTfBuilderId);

  for (auto &lStfSenderIdCli : mStfSenderRpcClients) {
    const auto &lStfSenderId = lStfSenderIdCli.first;
    auto &lStfSenderRpcCli = lStfSenderIdCli.second;

    StatusResponse lResponse;
    if(!lStfSenderRpcCli->DisconnectTfBuilderRequest(lParam, lResponse).ok()) {
      DDLOG(fair::Severity::ERROR) << "TfBuilder Connection error: gRPC error when connecting StfSender " << lStfSenderId << " to " << pTfBuilderId;
    }

    // check StfSender status
    if (lResponse.status() != 0) {
      DDLOG(fair::Severity::ERROR) << "DisconnectTfBuilderRequest( StfSender: " << lStfSenderId
        << ", TfBuilder: " << pTfBuilderId << "): response status: " << lResponse.status();
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
        // gRPC problem... continue asking for other STFs
        DDLOG(fair::Severity::WARNING) << "StfSender (" << lStfSenderId << ") gRPC connection problem. Code: "
                      <<lStatus.error_code() << ", message: " << lStatus.error_message();
      }

      if (lStfResponse.status() == StfDataResponse::DATA_DROPPED_TIMEOUT) {
        DDLOG(fair::Severity::WARNING) << "StfSender " << lStfSenderId << " dropped STF " << pLamStfId <<
                         " before notification from TfScheduler. Check StfSender buffer state.";
      } else if (lStfResponse.status() == StfDataResponse::DATA_DROPPED_UNKNOWN) {
        DDLOG(fair::Severity::WARNING) << "StfSender " << lStfSenderId << " dropped STF " << pLamStfId << " for unknown reason"
                         " before notification from TfScheduler. Check StfSender buffer state.";
      }
    }

    return pLamStfId;
  };

  try {
    auto lFeature = std::async(std::launch::async, lDropLambda, pStfId);

    std::scoped_lock lLock(mStfDropFuturesLock);
    mStfDropFutures.emplace_back(std::move(lFeature));

  } catch (std::exception &) {
    DDLOG(fair::Severity::WARNING) << "dropAllStfsAsync: async method failed. Calling synchronously.";
    lDropLambda(pStfId);
  }

}

void TfSchedulerConnManager::StfSenderMonitoringThread()
{
  DDLOG(fair::Severity::DEBUG) << "Starting StfSender RPC Monitoring thread...";
  // wait for the device to go into RUNNING state
  //
  std::vector<std::uint64_t> lDroppedStfs;

  while (mRunning) {
    // make sure all StfSenders are alive
    const std::uint32_t lNumStfSenders = checkStfSenders();
    if (lNumStfSenders < mPartitionInfo.mStfSenderIdList.size()) {
      DDLOG(fair::Severity::INFO) << "Connected StfSenders: " << lNumStfSenders << " out of " << mPartitionInfo.mStfSenderIdList.size();
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
        static std::uint64_t lsDropLogRate = 0;
        if (++lsDropLogRate % 256 == 0) {
          DDLOG(fair::Severity::INFO) << "Dropped SubTimeFrame (cannot schedule): " << lDroppedId << ", total: " << lsDropLogRate;
        }
      }
      lDroppedStfs.clear();
    }

    std::this_thread::sleep_for(1000ms);
  }

  DDLOG(fair::Severity::DEBUG) << "Exiting StfSender RPC Monitoring thread...";
}


}
} /* o2::DataDistribution */
