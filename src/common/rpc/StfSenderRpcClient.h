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

#ifndef ALICEO2_DATADIST_StfSender_RPC_CLIENT_H_
#define ALICEO2_DATADIST_StfSender_RPC_CLIENT_H_

#include "ConfigConsul.h"

#include <DataDistMonitoring.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <discovery.pb.h>
#include <discovery.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#pragma GCC diagnostic pop

#include <vector>
#include <map>
#include <thread>

namespace o2::DataDistribution
{

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Status;


class StfSenderRpcClient {
public:
  StfSenderRpcClient() = delete;
  StfSenderRpcClient(const std::string &pEndpoint);

  // rpc ConnectTfBuilderRequest(TfBuilderEndpoint) returns (ConnectTfBuilderResponse) { }
  grpc::Status ConnectTfBuilderRequest(const TfBuilderEndpoint &pParam, ConnectTfBuilderResponse &pRet /*out*/) {
    ClientContext lContext;
    return mStub->ConnectTfBuilderRequest(&lContext, pParam, &pRet);
  }

  // rpc DisconnectTfBuilderRequest(TfBuilderEndpoint) returns (StatusResponse) { }
  grpc::Status DisconnectTfBuilderRequest(const TfBuilderEndpoint &pParam, StatusResponse &pRet /*out*/) {
    ClientContext lContext;
    return mStub->DisconnectTfBuilderRequest(&lContext, pParam, &pRet);
  }

  // Connect UCX channels
  //rpc ConnectTfBuilderUCXRequest(TfBuilderUCXEndpoint) returns (ConnectTfBuilderResponse) { }
  grpc::Status ConnectTfBuilderUCXRequest(const TfBuilderUCXEndpoint &pParam, ConnectTfBuilderUCXResponse &pRet) {
    ClientContext lContext;
    return mStub->ConnectTfBuilderUCXRequest(&lContext, pParam, &pRet);
  }

  //rpc DisconnectTfBuilderUCXRequest(TfBuilderUCXEndpoint) returns (StatusResponse) { }
  grpc::Status DisconnectTfBuilderUCXRequest(const TfBuilderUCXEndpoint &pParam, StatusResponse &pRet) {
    ClientContext lContext;
    return mStub->DisconnectTfBuilderUCXRequest(&lContext, pParam, &pRet);
  }

  // rpc StfDataRequest(StfDataRequestMessage) returns (StfDataResponse) { }
  grpc::Status StfDataRequest(const StfDataRequestMessage &pParam, StfDataResponse &pRet /*out*/) {
    auto lStart = std::chrono::steady_clock::now();

    ClientContext lContext;
    // const auto lDeadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
    // lContext.set_deadline(lDeadline);
    lContext.set_wait_for_ready(false);
    auto lRet = mStub->StfDataRequest(&lContext, pParam, &pRet);

    if (mMonitorDuration) {
      DDMON("datadist.grpc", "StfDataRequest_ms", since<std::chrono::milliseconds>(lStart));
    }

    return lRet;
  }

  grpc::Status StfDataDropRequest(const StfDataRequestMessage &pParam, StfDataResponse &pRet /*out*/) {
    auto lStart = std::chrono::steady_clock::now();

    ClientContext lContext;
    // const auto lDeadline = std::chrono::system_clock::now() + std::chrono::milliseconds(1000);
    // lContext.set_deadline(lDeadline);
    lContext.set_wait_for_ready(false);
    auto lRet = mStub->StfDataRequest(&lContext, pParam, &pRet);

    if (mMonitorDuration) {
      DDMON("datadist.grpc", "StfDataDropRequest_ms", since<std::chrono::milliseconds>(lStart));
    }

    return lRet;
  }

  // rpc TerminatePartition(PartitionInfo) returns (PartitionResponse) { }
  bool TerminatePartition() {
    ClientContext lContext;
    PartitionInfo lPartInfo;
    PartitionResponse lRet;

    const auto lStatus = mStub->TerminatePartition(&lContext, lPartInfo, &lRet);
    return lStatus.ok(); // could have been stopped by the ECS
  }

  bool is_ready() const;
  bool is_alive() const {
    if (mChannel) {
      return (mChannel->GetState(true) != grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN);
    }
    return false;
  }

  std::string grpc_status();

  void setMonitorDuration(const bool pMonitor) { mMonitorDuration = pMonitor; };

private:
  std::unique_ptr<StfSenderRpc::Stub> mStub;
  std::shared_ptr<grpc::Channel> mChannel;

  // monitoring
  bool mMonitorDuration = false;
};

template <class T>
class StfSenderRpcClientCollection {
public:
  StfSenderRpcClientCollection(std::shared_ptr<T> pDiscoveryConfig)
  : mDiscoveryConfig(pDiscoveryConfig)
  { }

  ~StfSenderRpcClientCollection() { if (mRunning) { stop(); } }

  bool start()
  {
    using namespace std::chrono_literals;
    const auto &lPartId = mDiscoveryConfig->status().partition().partition_id();

    if (lPartId.empty()) {
      return false;
    }

    // try to connect to all StfSenders until stop is called
    mRunning = true;

    std::size_t lNumStfSenders = 0;

    // try to connect to all StfSenders gRPC endpoints

    // get a set of missing StfSenders
    TfSchedulerConfigStatus lSchedulerInst;
    if (! mDiscoveryConfig->getTfSchedulerConfig(lPartId, lSchedulerInst /*out*/)) {
      IDDLOG_RL(2000, "TfScheduler is not running. partition={}", lPartId);
      return false;
    }

    {
      std::unique_lock lLock(mClientsGlobalLock);
      lNumStfSenders = lSchedulerInst.stf_sender_id_list().size();
      DDDLOG("Connecting gRPC clients. stfs_num={} configured_num={}", lNumStfSenders, mClients.size());
    }

    // Connect to all StfSenders
    for (const std::string &lStfSenderId : lSchedulerInst.stf_sender_id_list()) {
      std::unique_lock lLock(mClientsGlobalLock);

      // check if already connected
      if (mClients.count(lStfSenderId) == 1) {
        continue;
      }

      StfSenderConfigStatus lStfSenderStatus;
      if (! mDiscoveryConfig->getStfSenderConfig(lPartId, lStfSenderId, lStfSenderStatus /*out*/)) {
        DDDLOG("Missing StfSender configuration. Connection will be retried. stfs_id={}", lStfSenderId);
        continue;
      }

      if (lStfSenderStatus.rpc_endpoint().empty()) {
        DDDLOG("StfSender rpc_endpoint field empty. stfs_id={}", lStfSenderId);
        continue;
      }

      // create the RPC client
      mClients.try_emplace(
        lStfSenderId,
        std::make_unique<StfSenderRpcClient>(lStfSenderStatus.rpc_endpoint())
      );
    }

    // make sure all connections are established
    bool lAllConnReady = true;
    bool lWaitForStfSenders = false;
    {
      std::shared_lock lLock(mClientsGlobalLock);

      if (mClients.size() < lNumStfSenders) {
        lWaitForStfSenders = true;
        IDDLOG_RL(10000, "gRPC: Connected to {} out of {} StfSenders", mClients.size(), lNumStfSenders);
      }

      // check the connection on existing clients
      for (auto &[ mCliId, lClient] : mClients) {
        if (!lClient->is_ready()) {
          lAllConnReady = false;
          DDDLOG_RL(2000, "StfSender gRPC client connection is not ready. stfs_id={} grpc_status={}", mCliId, lClient->grpc_status());
        }
      }
    }

    // retry connecting all Clients
    if (lWaitForStfSenders) {
      // back off until gRPC servers on all StfSeners become ready
      std::this_thread::sleep_for(250ms);
      return false;
    }

    // only continue when all connections are established
    mClientsCreated = lAllConnReady;

    return lAllConnReady;
  }

  void stop()
  {
    mRunning = false;
    std::unique_lock lLock(mClientsGlobalLock);
    mClients.clear();
    mClientsCreated = false;
  }

  bool checkStfSenderRpcConn(const std::string &lStfSenderId)
  {
    std::shared_lock lLock(mClientsGlobalLock);
    if (mClientsCreated && mClients.count(lStfSenderId) == 1) {
      auto &lCli = mClients[lStfSenderId];
      return lCli->is_alive();
    }
    return false;
  }

  bool remove(const std::string pId)
  {
    std::unique_lock lLock(mClientsGlobalLock);

    if (mClients.count(pId) > 0) {
      mClients.erase(pId);
      return true;
    }
    return false;
  }

  std::size_t size() const { return mClients.size(); }
  std::size_t count(const std::string &pId) const { return mClients.count(pId); }
  auto& operator[](const std::string &pId) const { return mClients.at(pId); }

  auto begin() const { return mClients.begin(); }
  auto end() const { return mClients.end(); }

  bool started() const { return (mRunning && mClientsCreated); }

  void setMonitorDuration(const bool pMon) {
    std::shared_lock lLock(mClientsGlobalLock);
    for (auto &[ mCliId, lClient] : mClients) {
      (void) mCliId;
      lClient->setMonitorDuration(pMon);
    }
  }

private:

  std::atomic_bool mRunning = false;
  std::shared_ptr<T> mDiscoveryConfig;

  bool mClientsCreated = false;
  std::shared_mutex mClientsGlobalLock;
  std::map<std::string, std::unique_ptr<StfSenderRpcClient>> mClients;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_StfSender_RPC_CLIENT_H_ */
