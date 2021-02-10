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

#ifndef ALICEO2_DATADIST_TFSCHEDULER_RPC_CLIENT_H_
#define ALICEO2_DATADIST_TFSCHEDULER_RPC_CLIENT_H_

#include "ConfigConsul.h"

#include <discovery.pb.h>
#include <discovery.grpc.pb.h>
#include <grpcpp/grpcpp.h>

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


class TfSchedulerRpcClient {
public:
  TfSchedulerRpcClient() { }

  template <typename ConsulCli>
  bool start(std::shared_ptr<ConsulCli> pConfig) {

    const auto &lPartitionId = pConfig->status().partition().partition_id();

    using namespace std::chrono_literals;

    mTfSchedulerConf.Clear();
    if (!pConfig->getTfSchedulerConfig(lPartitionId, mTfSchedulerConf)) {
      IDDLOG("TfScheduler instance configuration not found.");
      return false;
    }

    const std::string &lEndpoint = mTfSchedulerConf.rpc_endpoint();

    mChannel = grpc::CreateChannel(lEndpoint, grpc::InsecureChannelCredentials());
    mStub = TfSchedulerInstanceRpc::NewStub(mChannel);

    IDDLOG("Connected to TfSchedulerInstance RPC endpoint={}", lEndpoint);

    return true;
  }

  void stop() {
    mTfSchedulerConf.Clear();
    mStub.reset(nullptr);
  }

  void updateTimeInformation(BasicInfo &pInfo);

  // rpc NumStfSendersInPartitionRequest(google.protobuf.Empty) returns (NumStfSendersInPartitionResponse) { }
  bool NumStfSendersInPartitionRequest(std::uint32_t &pNumStfSenders);

  // rpc TfBuilderConnectionRequest(TfBuilderConfigStatus) returns (TfBuilderConnectionResponse) { }
  bool TfBuilderConnectionRequest(TfBuilderConfigStatus &pParam, TfBuilderConnectionResponse &pRet /*out*/);

  // rpc TfBuilderDisconnectionRequest(TfBuilderConfigStatus) returns (StatusResponse) { }
  bool TfBuilderDisconnectionRequest(TfBuilderConfigStatus &pParam, StatusResponse &pRet /*out*/);

  // rpc TfBuilderUpdate(TfBuilderUpdateMessage) returns (google.protobuf.Empty) { }
  bool TfBuilderUpdate(TfBuilderUpdateMessage &pMsg);

  // rpc StfSenderStfUpdate(StfSenderStfInfo) returns (SchedulerStfInfoResponse) { }
  bool StfSenderStfUpdate(StfSenderStfInfo &pMsg, SchedulerStfInfoResponse &pRet);

  std::string getEndpoint() { return mTfSchedulerConf.rpc_endpoint(); }

  bool is_ready();

private:
  TfSchedulerInstanceConfigStatus mTfSchedulerConf;

  std::unique_ptr<TfSchedulerInstanceRpc::Stub> mStub;
  std::shared_ptr<grpc::Channel> mChannel;
};


}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_TFSCHEDULER_RPC_CLIENT_H_ */
