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
      DDLOGF(fair::Severity::INFO, "TfScheduler instance configuration not found.");
      return false;
    }

    const std::string &lEndpoint = mTfSchedulerConf.rpc_endpoint();

    mStub = TfSchedulerInstanceRpc::NewStub(
      grpc::CreateChannel(lEndpoint, grpc::InsecureChannelCredentials()));

    DDLOGF(fair::Severity::INFO, "Connected to TfSchedulerInstance RPC endpoint={}", lEndpoint);

    return true;
  }

  void updateTimeInformation(BasicInfo &pInfo) {
    auto [lTimeStr, lTimet] = ConsulImpl::getCurrentTimeString();
    pInfo.set_last_update(lTimeStr);
    pInfo.set_last_update_t(lTimet);
  }

  void stop() {
    mTfSchedulerConf.Clear();
    mStub.reset(nullptr);
  }

  // rpc NumStfSendersInPartitionRequest(google.protobuf.Empty) returns (NumStfSendersInPartitionResponse) { }
  bool NumStfSendersInPartitionRequest(std::uint32_t &pNumStfSenders) {
    if (!mStub) {
      DDLOGF(fair::Severity::ERROR, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
      return false;
    }

    using namespace std::chrono_literals;
    do {
      ClientContext lContext;
      pNumStfSenders = 0;

      ::google::protobuf::Empty lParam;
      NumStfSendersInPartitionResponse lRet;

      auto lStatus = mStub->NumStfSendersInPartitionRequest(&lContext, lParam, &lRet);
      if (lStatus.ok()) {
        pNumStfSenders = lRet.num_stf_senders();
        return true;
      }
      if (lStatus.error_code() == grpc::StatusCode::UNAVAILABLE) {
          DDLOGF(fair::Severity::WARNING, "NumStfSendersInPartitionRequest: Scheduler gRPC server UNAVAILABLE. Retrying...");
          std::this_thread::sleep_for(250ms);
          continue; // retry
      }

      DDLOGF(fair::Severity::ERROR, "gRPC request error. code={} message={}", lStatus.error_code(), lStatus.error_message());
      break;

    } while (true);

    return false;
  }


  // rpc TfBuilderConnectionRequest(TfBuilderConfigStatus) returns (TfBuilderConnectionResponse) { }
  bool TfBuilderConnectionRequest(TfBuilderConfigStatus &pParam, TfBuilderConnectionResponse &pRet /*out*/) {
    if (!mStub) {
      DDLOGF(fair::Severity::ERROR, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
      return false;
    }

    using namespace std::chrono_literals;
    do {
      ClientContext lContext;

      // update timestamp
      updateTimeInformation(*pParam.mutable_info());

      auto lStatus = mStub->TfBuilderConnectionRequest(&lContext, pParam, &pRet);
      if (lStatus.ok()) {
        return true;
      }

      if (lStatus.error_code() == grpc::StatusCode::UNAVAILABLE) {
        std::this_thread::sleep_for(250ms);
        continue; // retry
      }

      pRet.Clear();
    } while (false);


    return false;
  }

  // rpc TfBuilderDisconnectionRequest(TfBuilderConfigStatus) returns (StatusResponse) { }
  bool TfBuilderDisconnectionRequest(TfBuilderConfigStatus &pParam, StatusResponse &pRet /*out*/) {
    if (!mStub) {
      DDLOGF(fair::Severity::ERROR, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
      return false;
    }

    using namespace std::chrono_literals;
    do {
      ClientContext lContext;

      // update timestamp
      updateTimeInformation(*pParam.mutable_info());

      auto lStatus = mStub->TfBuilderDisconnectionRequest(&lContext, pParam, &pRet);
      if (lStatus.ok()) {
        return true;
      }

      if (lStatus.error_code() == grpc::StatusCode::UNAVAILABLE) {
        std::this_thread::sleep_for(500ms);
        continue; // retry
      }

      pRet.Clear();
    } while (false);


    return false;
  }

  // rpc TfBuilderUpdate(TfBuilderUpdateMessage) returns (google.protobuf.Empty) { }
  bool TfBuilderUpdate(TfBuilderUpdateMessage &pMsg) {
    if (!mStub) {
      DDLOGF(fair::Severity::ERROR, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
      return false;
    }

    using namespace std::chrono_literals;

    ClientContext lContext;
    ::google::protobuf::Empty lRet;

    // update timestamp
    updateTimeInformation(*pMsg.mutable_info());

    auto lStatus = mStub->TfBuilderUpdate(&lContext, pMsg, &lRet);
    if (lStatus.ok()) {
      return true;
    }

    DDLOGF(fair::Severity::ERROR, "gRPC: TfBuilderUpdate error. code={} message={}",
      lStatus.error_code(), lStatus.error_message());
    return false;
  }


  // rpc StfSenderStfUpdate(StfSenderStfInfo) returns (SchedulerStfInfoResponse) { }
  bool StfSenderStfUpdate(StfSenderStfInfo &pMsg, SchedulerStfInfoResponse &pRet) {
    if (!mStub) {
      DDLOGF(fair::Severity::ERROR, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
      return false;
    }

    ClientContext lContext;

    // update timestamp
    updateTimeInformation(*pMsg.mutable_info());

    auto lStatus = mStub->StfSenderStfUpdate(&lContext, pMsg, &pRet);
    if (lStatus.ok()) {
      return true;
    }

    DDLOGF(fair::Severity::ERROR, "gRPC: StfSenderStfUpdate error. code={} message={}",
      lStatus.error_code(), lStatus.error_message());
    return false;
  }

  std::string getEndpoint() { return mTfSchedulerConf.rpc_endpoint(); }

private:
  TfSchedulerInstanceConfigStatus mTfSchedulerConf;

  std::unique_ptr<TfSchedulerInstanceRpc::Stub> mStub;
};


}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_TFSCHEDULER_RPC_CLIENT_H_ */
