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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <discovery.pb.h>
#include <discovery.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#pragma GCC diagnostic pop

#include <vector>
#include <map>
#include <thread>

namespace o2
{
namespace DataDistribution
{

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Status;


class StfSenderRpcClient {
public:
  StfSenderRpcClient(const std::string &pEndpoint)
  :
  mStub(StfSenderRpc::NewStub(grpc::CreateChannel(pEndpoint, grpc::InsecureChannelCredentials())))
  {
    // Check the connection...

  }

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

  // rpc StfDataRequest(StfDataRequestMessage) returns (StfDataResponse) { }
  grpc::Status StfDataRequest(const StfDataRequestMessage &pParam, StfDataResponse &pRet /*out*/) {
    ClientContext lContext;
    return mStub->StfDataRequest(&lContext, pParam, &pRet);
  }

private:
  std::unique_ptr<StfSenderRpc::Stub> mStub;
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
    TfSchedulerInstanceConfigStatus lSchedulerInst;
    if (! mDiscoveryConfig->getTfSchedulerConfig(lPartId, lSchedulerInst /*out*/)) {
      EDDLOG("TfScheduler is not running. partition={}", lPartId);

      return false;
    }

    lNumStfSenders = lSchedulerInst.stf_sender_id_list().size();
    DDDLOG("Connecting gRPC clients. stfs_id={}", lNumStfSenders);

    // Connect to all StfSenders
    for (const std::string &lStfSenderId : lSchedulerInst.stf_sender_id_list()) {

      // check if already connected
      if (mClients.count(lStfSenderId) == 1) {
        continue;
      }

      StfSenderConfigStatus lStfSenderStatus;
      if (! mDiscoveryConfig->getStfSenderConfig(lPartId, lStfSenderId, lStfSenderStatus /*out*/)) {
        DDDLOG("Missing StfSender configuration. Connection will be retried. stfs_id={}",
          lStfSenderId);
        continue;
      }

      if (lStfSenderStatus.rpc_endpoint().empty()) {
        EDDLOG("StfSender rpc_endpoint field empty. stfs_id={}", lStfSenderId);
        continue;
      }

      // create the RPC client
      mClients.try_emplace(
        lStfSenderId,
        std::make_unique<StfSenderRpcClient>(lStfSenderStatus.rpc_endpoint())
      );
    }

    IDDLOG("gRPC: Connected to {}/{} StfSender.", mClients.size(), lNumStfSenders);

    if (mClients.size() != lNumStfSenders) {
      static int sBackoff = 0;
      sBackoff = std::min(sBackoff + 1, 20);
      // back off until gRPC server on all StfSeners becomes ready
      std::this_thread::sleep_for(sBackoff * 100ms);
    }

    // retry connecting all Clients
    if (!mRunning || mClients.size() < lNumStfSenders) {
      return false;
    }

    return true;
  }

  void stop()
  {
    mRunning = false;
    mClients.clear();

  }

  void checkStfSenderRpcConn(const std::string &lStfSenderId)
  {
    if (mClients.count(lStfSenderId) == 1) {
      start();
    }
  }

  std::size_t size() const { return mClients.size(); }
  std::size_t count(const std::string &pId) const { return mClients.count(pId); }
  auto& operator[](const std::string &pId) const { return mClients.at(pId); }

  auto begin() const { return mClients.begin(); }
  auto end() const { return mClients.end(); }

private:

  std::atomic_bool mRunning = false;

  std::shared_ptr<T> mDiscoveryConfig;

  std::map<std::string, std::unique_ptr<StfSenderRpcClient>> mClients;
};

}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_StfSender_RPC_CLIENT_H_ */

