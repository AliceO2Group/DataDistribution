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

#include "TfSchedulerInstanceRpc.h"

#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <stdexcept>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

void TfSchedulerInstanceRpcImpl::initDiscovery(const std::string pRpcSrvBindIp, int &lRealPort)
{
  ServerBuilder lSrvBuilder;
  lSrvBuilder.AddListeningPort(pRpcSrvBindIp + ":0",
                                grpc::InsecureServerCredentials(),
                                &lRealPort  /*auto assigned port */);
  lSrvBuilder.RegisterService(this);

  assert(!mServer);
  mServer = lSrvBuilder.BuildAndStart();

  IDDLOG("gRPC server is staretd. server_ep={}:{}", pRpcSrvBindIp, lRealPort);
}

void TfSchedulerInstanceRpcImpl::start()
{
  mRunning = true;
  mMonitorThread = create_thread_member("part_monitor", &TfSchedulerInstanceRpcImpl::PartitionMonitorThread, this);

  assert(mServer);
  // start TfBuilder info database
  mTfBuilderInfo.start();

  // start StfInfo database
  mStfInfo.start();

  // start all client gRPC channels
  // This can block, waiting to connect to all StfSenders.
  // We have to loop and check if we should bail on Terminate request
  while (accepting_updates() && !mConnManager.start()) {
    std::this_thread::sleep_for(500ms);
  }
}

void TfSchedulerInstanceRpcImpl::stop()
{
  mConnManager.stop();
  mStfInfo.stop();
  mTfBuilderInfo.stop();

  mRunning = false;
  if (mMonitorThread.joinable()) {
    mMonitorThread.join();
  }

  if (mServer) {
    mServer->Shutdown();
    mServer.reset(nullptr);
  }
}

void TfSchedulerInstanceRpcImpl::updatePartitionState(const PartitionState pNewState)
{
  if (pNewState != mPartitionState) {
    IDDLOG("PartitionState: Changing partition state from '{}' to '{}'",
      PartitionState_Name(mPartitionState), PartitionState_Name(pNewState));
    mPartitionState = pNewState;

    // persist the new partition state
    mDiscoveryConfig->status().set_partition_state(mPartitionState);
    mDiscoveryConfig->write();
  }
}

void TfSchedulerInstanceRpcImpl::PartitionMonitorThread()
{
  while (mRunning) {
    std::this_thread::sleep_for(500ms);

    // In teardown?
    if (mPartitionState == PartitionState::PARTITION_TERMINATING) {
      // Notify TfBuilders
      if (mConnManager.requestTfBuildersTerminate()) {
        IDDLOG("PartitionMonitorThread: All TfBuilders requested to terminate.");
      }

      if (mConnManager.requestStfSendersTerminate()) {
        IDDLOG("PartitionMonitorThread: All StfSenders requested to terminate.");
        // safe to exit now
        mStfInfo.stop();

        updatePartitionState(PartitionState::PARTITION_TERMINATED);
        break;
      }

      // keep trying to terminate the partition
      continue;
    }

    // check StfSender State
    switch (mConnManager.getStfSenderState()) {
    case StfSenderState::STF_SENDER_STATE_OK:
      updatePartitionState(PartitionState::PARTITION_CONFIGURED);
      break;
    case StfSenderState::STF_SENDER_STATE_INITIALIZING:
      updatePartitionState(PartitionState::PARTITION_CONFIGURING);
      break;
    case StfSenderState::STF_SENDER_STATE_INCOMPLETE:
      updatePartitionState(PartitionState::PARTITION_ERROR);
      break;
    default:
      updatePartitionState(PartitionState::PARTITION_ERROR);
      break;
    }
  }

  DDDLOG("PartitionMonitorThread: Exiting.");
}

::grpc::Status TfSchedulerInstanceRpcImpl::HeartBeat(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::BasicInfo* request, ::google::protobuf::Empty* /*response*/)
{
  static std::uint64_t sStfSendersHb = 0;
  static std::uint64_t sTfBuildersHb = 0;

  if (request) {
    if (request->type() == ProcessTypePB::StfSender) {
      sStfSendersHb++;
    } else if (request->type() == ProcessTypePB::TfBuilder) {
      sTfBuildersHb++;
    }
  }

  DDDLOG_GRL(10000, "HeartBeat: receiving. stfs_count={} tfb_count={}", sStfSendersHb, sTfBuildersHb);

  return Status::OK;
}

::grpc::Status TfSchedulerInstanceRpcImpl::GetPartitionState(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::PartitionInfo* /*request*/, ::o2::DataDistribution::PartitionResponse* response)
{
  DDDLOG("gRPC server: GetPartitionState");

  if (!accepting_updates()) {
    response->set_partition_state(mPartitionState);
    return Status::OK;
  }

  // Terminating?
  switch (mConnManager.getStfSenderState()) {
    case StfSenderState::STF_SENDER_STATE_OK:
    {
      response->set_partition_state(o2::DataDistribution::PartitionState::PARTITION_CONFIGURED);
      response->set_info_message("Partition is fully configured.");
      break;
    }
    case StfSenderState::STF_SENDER_STATE_INITIALIZING:
    {
      response->set_partition_state(o2::DataDistribution::PartitionState::PARTITION_CONFIGURING);
      const auto lMsg = fmt::format("Partition is being configured. Found {} out of {} StfSenders.",
        mConnManager.getStfSenderCount(), mConnManager.getStfSenderSet().size());
      response->set_info_message(lMsg);
      break;
    }
    case StfSenderState::STF_SENDER_STATE_INCOMPLETE:
    {
      response->set_partition_state(o2::DataDistribution::PartitionState::PARTITION_ERROR);
      response->set_info_message("Not all StfSenders are reachable.");
      break;
    }
    default:
    {
      response->set_partition_state(o2::DataDistribution::PartitionState::PARTITION_ERROR);
      response->set_info_message("Unknown partition state.");
    }
  }

  return Status::OK;
}


::grpc::Status TfSchedulerInstanceRpcImpl::TerminatePartition(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::PartitionInfo* request, ::o2::DataDistribution::PartitionResponse* response)
{
  IDDLOG("TerminatePartition: request to teardown partition {}", request->partition_id());

  if (accepting_updates()) {
    updatePartitionState(PartitionState::PARTITION_TERMINATING);
    response->set_info_message("Terminate started.");
  } else {
    const auto lMsg = fmt::format("Terminate is already requested. partition_id={}", request->partition_id());
    response->set_info_message(lMsg);
    WDDLOG(lMsg);
  }

  response->set_partition_state(mPartitionState);

  return Status::OK;
}


::grpc::Status TfSchedulerInstanceRpcImpl::NumStfSendersInPartitionRequest(::grpc::ServerContext* /*context*/,
  const ::google::protobuf::Empty* /*request*/, ::o2::DataDistribution::NumStfSendersInPartitionResponse* response)
{
  DDDLOG("gRPC server: NumStfSendersInPartitionRequest");

  if (!accepting_updates()) {
    return Status::CANCELLED;
  }

  response->set_num_stf_senders(mPartitionInfo.mStfSenderIdList.size());

  return Status::OK;
}


::grpc::Status TfSchedulerInstanceRpcImpl::TfBuilderConnectionRequest(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::TfBuilderConfigStatus* request,
  ::o2::DataDistribution::TfBuilderConnectionResponse* response)
{
  DDDLOG("gRPC server: TfBuilderConnectionRequest");

  if (!accepting_updates()) {
    response->set_status(TfBuilderConnectionStatus::ERROR_PARTITION_TERMINATING);
    return Status::OK;
  }

  mConnManager.connectTfBuilder(*request, *response /*out*/);
  return Status::OK;
}


::grpc::Status TfSchedulerInstanceRpcImpl::TfBuilderDisconnectionRequest(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::TfBuilderConfigStatus* request, ::o2::DataDistribution::StatusResponse* response)
{
  DDDLOG("gRPC server: TfBuilderDisconnectionRequest");

  mConnManager.disconnectTfBuilder(*request, *response /*out*/);

  return Status::OK;
}

::grpc::Status TfSchedulerInstanceRpcImpl::TfBuilderUpdate(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::TfBuilderUpdateMessage* request, ::google::protobuf::Empty* /*response*/)
{
  static std::atomic_uint64_t sTfBuilderUpdates = 0;

  if (!accepting_updates()) {
    return Status::OK;
  }

  sTfBuilderUpdates++;
  DDLOGF_RL(3000, DataDistSeverity::debug, "gRPC server: TfBuilderUpdate. tfb_id={} total={}",
    request->info().process_id(), sTfBuilderUpdates);

  mTfBuilderInfo.updateTfBuilderInfo(*request);

  return Status::OK;
}

::grpc::Status TfSchedulerInstanceRpcImpl::StfSenderStfUpdate(::grpc::ServerContext* /*context*/,
  const ::o2::DataDistribution::StfSenderStfInfo* request, ::o2::DataDistribution::SchedulerStfInfoResponse* response)
{
  static std::atomic_uint64_t sStfUpdates = 0;

  if (!accepting_updates()) {
    response->set_status(SchedulerStfInfoResponse::DROP_NOT_RUNNING);
    return Status::OK;
  }

  sStfUpdates++;
  DDLOGF_GRL(3000, DataDistSeverity::debug, "gRPC server: StfSenderStfUpdate. stfs_id={} total={}",
    request->info().process_id(), sStfUpdates);

  response->Clear();
  mStfInfo.addStfInfo(*request, *response /*out*/);

  return Status::OK;
}


}
} /* o2::DataDistribution */
