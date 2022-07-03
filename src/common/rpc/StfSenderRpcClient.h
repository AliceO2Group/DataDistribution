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
#include <shared_mutex>

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
    lContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10000));
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
    lContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10000));
    return mStub->DisconnectTfBuilderUCXRequest(&lContext, pParam, &pRet);
  }

  // rpc StfDataRequest(StfDataRequestMessage) returns (StfDataResponse) { }
  bool StfDataRequestTest() {
    auto lStart = std::chrono::steady_clock::now();

    int lRetries = 0;

    while (++lRetries < 5) {
      StfDataRequestMessage lParam;
      lParam.set_stf_id(0);
      lParam.set_tf_builder_id("-1");
      StfDataResponse lRet;
      ClientContext lContext;
      lContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));
      lContext.set_wait_for_ready(true);

      auto lRetVal = mStub->StfDataRequest(&lContext, lParam, &lRet);
      if (mMonitorDuration) {
        DDMON("datadist.grpc", "StfDataRequest_ms", since<std::chrono::milliseconds>(lStart));
      }

      if (lRetVal.error_code() == grpc::StatusCode::OK) {
        return true;
      }
    }

    return false;
  }

  // rpc StfDataRequest(StfDataRequestMessage) returns (StfDataResponse) { }
  grpc::Status StfDataRequest(const StfDataRequestMessage &pParam, StfDataResponse &pRet /*out*/) {
    auto lStart = std::chrono::steady_clock::now();

    ClientContext lContext;
    lContext.set_wait_for_ready(false);
    auto lRet = mStub->StfDataRequest(&lContext, pParam, &pRet);

    if (mMonitorDuration) {
      DDMON("datadist.grpc", "StfDataRequest_ms", since<std::chrono::milliseconds>(lStart));
    }

    return lRet;
  }

  // rpc StfDataRequest(StfDataRequestMessage) returns (StfDataResponse) { }
  template <typename Dur>
  grpc::Status StfDataRequestWithTimeout(const Dur pTimeout, const StfDataRequestMessage &pParam, StfDataResponse &pRet /*out*/) {
    auto lStart = std::chrono::steady_clock::now();

    ClientContext lContext;
    const auto lDeadline = std::chrono::system_clock::now() + pTimeout;
    lContext.set_deadline(lDeadline);
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
    const auto lDeadline = std::chrono::system_clock::now() + std::chrono::milliseconds(500);
    lContext.set_deadline(lDeadline);
    lContext.set_wait_for_ready(false);
    auto lRet = mStub->StfDataRequest(&lContext, pParam, &pRet);

    if (mMonitorDuration) {
      DDMON("datadist.grpc", "StfDataDropRequest_ms", since<std::chrono::milliseconds>(lStart));
    }

    return lRet;
  }

  // rpc TerminatePartition(PartitionInfo) returns (PartitionResponse) { }
  bool TerminatePartition(const PartitionInfo &pPartInfo) {
    ClientContext lContext;
    lContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));

    PartitionResponse lRet;
    mStub->TerminatePartition(&lContext, pPartInfo, &lRet);
    return true; // could have been stopped by the ECS
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

      mClients[lStfSenderId]->setMonitorDuration(mMonitorRpcDuration);
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
      std::this_thread::sleep_for(150ms);
      return false;
    }

    // test if actual connections are performing on StfSender
    // NOTE: we cannot fail TfBuilder because the whole run will fail until nmin ODC/DDS is implemented
    //       TfBuilder must be transition to Ready, but the input will be disabled
    bool lConnWorking = checkStfSenderRpcConn();

    auto lMsg = "StfSender gRPC connection finished. success={} failed={}";
    if (lConnWorking) {
      IDDLOG(lMsg, lConnWorking, getNumConnectedClients() - getNumWorkingClients());
    } else {
      EDDLOG(lMsg, lConnWorking, getNumConnectedClients() - getNumWorkingClients());
    }

    // only continue when all connections are established
    mClientsCreated = lAllConnReady;

    return mClientsCreated;
  }

  void stop()
  {
    mRunning = false;
    std::unique_lock lLock(mClientsGlobalLock);
    mClients.clear();
    mClientsCreated = false;
  }

  bool checkStfSenderRpcConn() {
    std::shared_lock lLock(mClientsGlobalLock);

    bool lConnWorking = true;
    mNumWorkingClients = 0;

    for (auto &[ mCliId, lClient] : mClients) {
      // attempt the test StfDataRequest()
      if (!lClient->StfDataRequestTest()) {
        EDDLOG("StfSender gRPC connection is not working. stfs_id={} grpc_status={}", mCliId, lClient->grpc_status());
        lConnWorking = false;
        continue;
      }
      mNumWorkingClients += 1;
    }

    return lConnWorking;
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
  // auto& operator[](const std::string &pId) const { return mClients.at(pId); }

  template <typename... Args>
  auto StfDataRequest(const std::string &pId, Args&&... args) {
    std::shared_lock lLock(mClientsGlobalLock);
    return mClients.at(pId)->StfDataRequest(std::forward<Args>(args)...);
  }

  template <typename Dur, typename... Args>
  auto StfDataRequestWithTimeout(const std::string &pId, const Dur pTimeout, Args&&... args) {
    std::shared_lock lLock(mClientsGlobalLock);
    return mClients.at(pId)->StfDataRequestWithTimeout(pTimeout, std::forward<Args>(args)...);
  }

  template <typename... Args>
  auto StfDataDropRequest(const std::string &pId, Args&&... args) {
    std::shared_lock lLock(mClientsGlobalLock);
    return mClients.at(pId)->StfDataDropRequest(std::forward<Args>(args)...);
  }

  template <typename... Args>
  auto TerminatePartition(const std::string &pId, Args&&... args) {
    std::shared_lock lLock(mClientsGlobalLock);
    return mClients.at(pId)->TerminatePartition(std::forward<Args>(args)...);
  }

  template <typename... Args>
  auto ConnectTfBuilderRequest(const std::string &pId, Args&&... args) {
    std::shared_lock lLock(mClientsGlobalLock);
    return mClients.at(pId)->ConnectTfBuilderRequest(std::forward<Args>(args)...);
  }

  template <typename... Args>
  auto ConnectTfBuilderUCXRequest(const std::string &pId, Args&&... args) {
    std::shared_lock lLock(mClientsGlobalLock);
    return mClients.at(pId)->ConnectTfBuilderUCXRequest(std::forward<Args>(args)...);
  }

  template <typename... Args>
  auto DisconnectTfBuilderRequest(const std::string &pId, Args&&... args) {
    std::shared_lock lLock(mClientsGlobalLock);
    return mClients.at(pId)->DisconnectTfBuilderRequest(std::forward<Args>(args)...);
  }

  auto begin() const { return mClients.begin(); }
  auto end() const { return mClients.end(); }

  bool started() const { return (mRunning && mClientsCreated); }

  void setMonitorDuration(const bool pMon) {
    mMonitorRpcDuration = pMon;
    std::shared_lock lLock(mClientsGlobalLock);
    for (auto &[ mCliId, lClient] : mClients) {
      (void) mCliId;
      lClient->setMonitorDuration(mMonitorRpcDuration);
    }
  }

  unsigned getNumWorkingClients() const { return mNumWorkingClients; }
  unsigned getNumConnectedClients() const { return mClients.size(); }

  std::vector<std::string> getStfSenderIds() const {
    std::shared_lock lLock(mClientsGlobalLock);
    std::vector<std::string> lRetVec;

    for (const auto &lCliIt : mClients) {
      lRetVec.push_back(lCliIt.first);
    }
    return lRetVec;
  }

private:

  std::atomic_bool mRunning = false;
  std::shared_ptr<T> mDiscoveryConfig;

  bool mClientsCreated = false;
  unsigned mNumWorkingClients = 0;

  mutable std::shared_mutex mClientsGlobalLock;
  std::map<std::string, std::unique_ptr<StfSenderRpcClient>> mClients;

  // monitoring
  bool mMonitorRpcDuration = false;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_StfSender_RPC_CLIENT_H_ */
