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

#include "TfSchedulerRpcClient.h"

#include <string>
#include <chrono>
#include <set>


namespace o2::DataDistribution
{

bool TfSchedulerRpcClient::is_ready() const {
  if (!mChannel) {
    return false;
  }

  return (mChannel->GetState(true) == grpc_connectivity_state::GRPC_CHANNEL_READY);
}

void TfSchedulerRpcClient::updateTimeInformation(BasicInfo &pInfo) {
  auto [lTimeStr, lTimet] = ConsulImpl::getCurrentTimeString();
  pInfo.set_last_update(lTimeStr);
  pInfo.set_last_update_t(lTimet);
}

// rpc HeartBeat(BasicInfo) returns (google.protobuf.Empty) { }
bool TfSchedulerRpcClient::HeartBeat(const BasicInfo &pInfo) {
  using namespace std::chrono_literals;
  if (is_alive()) {
    ClientContext lContext;
    ::google::protobuf::Empty lRet;
    // NumStfSendersInPartitionResponse lRet;
    BasicInfo lInfo = pInfo;

    const auto lStatus = mStub->HeartBeat(&lContext, lInfo, &lRet);
    if (lStatus.ok()) {
      return true;
    }

    if (!is_ready()) {
      IDDLOG_GRL(1000, "HeartBeat: Scheduler gRPC server is not ready. Retrying...");
      return false;
    }

    EDDLOG_GRL(1000, "HeartBeat: gRPC request error. code={} message={}", lStatus.error_code(), lStatus.error_message());
  }

  return false;
}

// rpc NumStfSendersInPartitionRequest(google.protobuf.Empty) returns (NumStfSendersInPartitionResponse) { }
bool TfSchedulerRpcClient::NumStfSendersInPartitionRequest(std::uint32_t &pNumStfSenders) {
  if (!mStub || !is_alive()) {
    WDDLOG_GRL(2000, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
    return false;
  }

  using namespace std::chrono_literals;
  while (is_alive()) {
    ClientContext lContext;
    pNumStfSenders = 0;

    ::google::protobuf::Empty lParam;
    NumStfSendersInPartitionResponse lRet;

    auto lStatus = mStub->NumStfSendersInPartitionRequest(&lContext, lParam, &lRet);
    if (lStatus.ok()) {
      pNumStfSenders = lRet.num_stf_senders();
      return true;
    }

    if (!is_ready()) {
      WDDLOG("NumStfSendersInPartitionRequest: Scheduler gRPC server is not ready. Retrying...");
      std::this_thread::sleep_for(250ms);
      continue; // retry
    }

    EDDLOG_GRL(1000, "gRPC request error. code={} message={}", lStatus.error_code(), lStatus.error_message());
    break;
  }

  return false;
}

// rpc TfBuilderConnectionRequest(TfBuilderConfigStatus) returns (TfBuilderConnectionResponse) { }
bool TfSchedulerRpcClient::TfBuilderConnectionRequest(TfBuilderConfigStatus &pParam, TfBuilderConnectionResponse &pRet /*out*/) {
  if (!mStub || !is_alive()) {
    WDDLOG_GRL(2000, "TfBuilderConnectionRequest: no gRPC connection to scheduler");
    return false;
  }

  using namespace std::chrono_literals;
  while (is_alive()) {
    ClientContext lContext;

    // update timestamp
    updateTimeInformation(*pParam.mutable_info());

    auto lStatus = mStub->TfBuilderConnectionRequest(&lContext, pParam, &pRet);

    if (lStatus.ok()) {

      switch (pRet.status()) {
        case TfBuilderConnectionStatus::OK:
          break;
        case TfBuilderConnectionStatus::ERROR_DISCOVERY:
          break;
        case TfBuilderConnectionStatus::ERROR_SOCKET_COUNT:
          EDDLOG("TfBuilderConnectionRequest: TfBuilder socket count is not equal to number of StfSenders.");
          break;
        case TfBuilderConnectionStatus::ERROR_STF_SENDERS_NOT_READY:
          break;
        case TfBuilderConnectionStatus::ERROR_GRPC_STF_SENDER:
          break;
        case TfBuilderConnectionStatus::ERROR_GRPC_TF_BUILDER:
          break;
        case TfBuilderConnectionStatus::ERROR_STF_SENDER_CONNECTING:
          break;
        case TfBuilderConnectionStatus::ERROR_STF_SENDER_EXISTS:
          EDDLOG("TfBuilderConnectionRequest: TfBuilder with the same id already registered with StfSenders.");
          break;
        case TfBuilderConnectionStatus::ERROR_PARTITION_TERMINATING:
          EDDLOG("TfBuilderConnectionRequest: Partition is terminating.");
          break;
        default:
          EDDLOG("TfBuilderConnectionRequest: Unknown error!");
          break;
      }

      return true;
    }

    pRet.Clear();
    std::this_thread::sleep_for(500ms);
  }

  return false;
}

// rpc TfBuilderDisconnectionRequest(TfBuilderConfigStatus) returns (StatusResponse) { }
bool TfSchedulerRpcClient::TfBuilderDisconnectionRequest(TfBuilderConfigStatus &pParam, StatusResponse &pRet /*out*/) {
  if (!mStub || !is_alive()) {
    WDDLOG_GRL(2000, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
    return false;
  }

  using namespace std::chrono_literals;
  ClientContext lContext;

  // update timestamp
  updateTimeInformation(*pParam.mutable_info());

  auto lStatus = mStub->TfBuilderDisconnectionRequest(&lContext, pParam, &pRet);
  if (!lStatus.ok()) {
    WDDLOG_GRL(2000, "TfBuilderDisconnectionRequest: gRPC call failed err={} details={}", lStatus.error_message(), lStatus.error_details());
    return false;
  }
  return true;
}

// rpc TfBuilderUCXConnectionRequest(TfBuilderConfigStatus) returns (TfBuilderUCXConnectionResponse) { }
bool TfSchedulerRpcClient::TfBuilderUCXConnectionRequest(TfBuilderConfigStatus &pParam, TfBuilderUCXConnectionResponse &pRet /*out*/) {
  if (!mStub || !is_alive()) {
    WDDLOG_GRL(2000, "TfBuilderUCXConnectionRequest: no gRPC connection to scheduler");
    return false;
  }

  using namespace std::chrono_literals;
  while (is_alive()) {
    ClientContext lContext;

    // update timestamp
    updateTimeInformation(*pParam.mutable_info());

    auto lStatus = mStub->TfBuilderUCXConnectionRequest(&lContext, pParam, &pRet);

    if (lStatus.ok()) {

      switch (pRet.status()) {
        case TfBuilderConnectionStatus::OK:
          break;
        case TfBuilderConnectionStatus::ERROR_DISCOVERY:
          break;
        case TfBuilderConnectionStatus::ERROR_SOCKET_COUNT:
          EDDLOG("TfBuilderUCXConnectionRequest: TfBuilder socket count is not equal to number of StfSenders.");
          break;
        case TfBuilderConnectionStatus::ERROR_STF_SENDERS_NOT_READY:
          break;
        case TfBuilderConnectionStatus::ERROR_GRPC_STF_SENDER:
          break;
        case TfBuilderConnectionStatus::ERROR_GRPC_TF_BUILDER:
          break;
        case TfBuilderConnectionStatus::ERROR_STF_SENDER_CONNECTING:
          break;
        case TfBuilderConnectionStatus::ERROR_STF_SENDER_EXISTS:
          EDDLOG("TfBuilderUCXConnectionRequest: TfBuilder with the same id already registered with StfSenders.");
          break;
        case TfBuilderConnectionStatus::ERROR_PARTITION_TERMINATING:
          EDDLOG("TfBuilderUCXConnectionRequest: Partition is terminating.");
          break;
        default:
          EDDLOG("TfBuilderUCXConnectionRequest: Unknown error!");
          break;
      }

      return true;
    }

    pRet.Clear();
    std::this_thread::sleep_for(500ms);
  }

  return false;
}

// rpc TfBuilderUCXDisconnectionRequest(TfBuilderConfigStatus) returns (StatusResponse) { }
bool TfSchedulerRpcClient::TfBuilderUCXDisconnectionRequest(TfBuilderConfigStatus &pParam, StatusResponse &pRet /*out*/) {
  if (!mStub || !is_alive()) {
    DDDLOG_GRL(2000, "TfBuilderUCXDisconnectionRequest: no gRPC connection to scheduler");
    return false;
  }

  using namespace std::chrono_literals;
  ClientContext lContext;

  // update timestamp
  updateTimeInformation(*pParam.mutable_info());

  auto lStatus = mStub->TfBuilderUCXDisconnectionRequest(&lContext, pParam, &pRet);
  if (!lStatus.ok()) {
    WDDLOG_GRL(2000, "TfBuilderUCXDisconnectionRequest: gRPC call failed err={} details={}", lStatus.error_message(), lStatus.error_details());
    return false;
  }
  return true;
}

// rpc TfBuilderUpdate(TfBuilderUpdateMessage) returns (google.protobuf.Empty) { }
bool TfSchedulerRpcClient::TfBuilderUpdate(TfBuilderUpdateMessage &pMsg) {
  if (!mStub || !is_alive()) {
    WDDLOG_GRL(2000, "TfBuilderUpdate: no gRPC connection to scheduler");
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

  EDDLOG_GRL(2000, "gRPC: TfBuilderUpdate error. code={} message={}", lStatus.error_code(), lStatus.error_message());
  return false;
}


// rpc StfSenderStfUpdate(StfSenderStfInfo) returns (SchedulerStfInfoResponse) { }
bool TfSchedulerRpcClient::StfSenderStfUpdate(StfSenderStfInfo &pMsg, SchedulerStfInfoResponse &pRet) {
  if (!mStub || !is_alive()) {
    WDDLOG_GRL(2000, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
    return false;
  }

  ClientContext lContext;

  // update timestamp
  updateTimeInformation(*pMsg.mutable_info());

  auto lStatus = mStub->StfSenderStfUpdate(&lContext, pMsg, &pRet);
  if (lStatus.ok()) {
    return true;
  }

  EDDLOG_GRL(2000, "gRPC: StfSenderStfUpdate error. code={} message={}", lStatus.error_code(), lStatus.error_message());
  return false;
}

}
