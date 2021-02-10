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


namespace o2
{
namespace DataDistribution
{

bool TfSchedulerRpcClient::is_ready() {
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

// rpc NumStfSendersInPartitionRequest(google.protobuf.Empty) returns (NumStfSendersInPartitionResponse) { }
bool TfSchedulerRpcClient::NumStfSendersInPartitionRequest(std::uint32_t &pNumStfSenders) {
  if (!mStub) {
    EDDLOG_GRL(1000, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
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
        WDDLOG("NumStfSendersInPartitionRequest: Scheduler gRPC server UNAVAILABLE. Retrying...");
        std::this_thread::sleep_for(250ms);
        continue; // retry
    }

    EDDLOG_GRL(1000, "gRPC request error. code={} message={}", lStatus.error_code(), lStatus.error_message());
    break;

  } while (true);

  return false;
}


// rpc TfBuilderConnectionRequest(TfBuilderConfigStatus) returns (TfBuilderConnectionResponse) { }
bool TfSchedulerRpcClient::TfBuilderConnectionRequest(TfBuilderConfigStatus &pParam, TfBuilderConnectionResponse &pRet /*out*/) {
  if (!mStub) {
    EDDLOG_GRL(1000, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
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
bool TfSchedulerRpcClient::TfBuilderDisconnectionRequest(TfBuilderConfigStatus &pParam, StatusResponse &pRet /*out*/) {
  if (!mStub) {
    EDDLOG_GRL(1000, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
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
bool TfSchedulerRpcClient::TfBuilderUpdate(TfBuilderUpdateMessage &pMsg) {
  if (!mStub) {
    EDDLOG_GRL(1000, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
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

  EDDLOG_GRL(1000, "gRPC: TfBuilderUpdate error. code={} message={}",
    lStatus.error_code(), lStatus.error_message());
  return false;
}


// rpc StfSenderStfUpdate(StfSenderStfInfo) returns (SchedulerStfInfoResponse) { }
bool TfSchedulerRpcClient::StfSenderStfUpdate(StfSenderStfInfo &pMsg, SchedulerStfInfoResponse &pRet) {
  if (!mStub) {
    EDDLOG_GRL(1000, "NumStfSendersInPartitionRequest: no gRPC connection to scheduler");
    return false;
  }

  ClientContext lContext;

  // update timestamp
  updateTimeInformation(*pMsg.mutable_info());

  auto lStatus = mStub->StfSenderStfUpdate(&lContext, pMsg, &pRet);
  if (lStatus.ok()) {
    return true;
  }

  EDDLOG_GRL(1000, "gRPC: StfSenderStfUpdate error. code={} message={}",
    lStatus.error_code(), lStatus.error_message());
  return false;
}


}
}
