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

#include <FairMQLogger.h>

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
  mServer = std::move(lSrvBuilder.BuildAndStart());

  LOG(INFO) << "gRPC server listening on : " << pRpcSrvBindIp << ":" << lRealPort;
}

void TfSchedulerInstanceRpcImpl::start()
{
  assert(mServer);
  // start TfBuilder info database
  mTfBuilderInfo.start();

  // start StfInfo database
  mStfInfo.start();

  // start all client gRPC channels
  mConnManager.start();
}

void TfSchedulerInstanceRpcImpl::stop()
{
  mConnManager.stop();
  mStfInfo.stop();
  mTfBuilderInfo.stop();

  if (mServer) {
    mServer->Shutdown();
    mServer.reset(nullptr);
  }
}


::grpc::Status TfSchedulerInstanceRpcImpl::NumStfSendersInPartitionRequest(::grpc::ServerContext* /*context*/, const ::google::protobuf::Empty* /*request*/, ::o2::DataDistribution::NumStfSendersInPartitionResponse* response)
{
  LOG(INFO) << "gRPC server: NumStfSendersInPartitionRequest";

  response->set_num_stf_senders(mPartitionInfo.mStfSenderIdList.size());

  return Status::OK;
}



::grpc::Status TfSchedulerInstanceRpcImpl::TfBuilderConnectionRequest(::grpc::ServerContext* /*context*/, const ::o2::DataDistribution::TfBuilderConfigStatus* request, ::o2::DataDistribution::TfBuilderConnectionResponse* response)
{
  LOG(INFO) << "gRPC server: TfBuilderConnectionRequest";

  mConnManager.connectTfBuilder(*request, *response /*out*/);

  return Status::OK;
}



::grpc::Status TfSchedulerInstanceRpcImpl::TfBuilderDisconnectionRequest(::grpc::ServerContext* /*context*/, const ::o2::DataDistribution::TfBuilderConfigStatus* request, ::o2::DataDistribution::StatusResponse* response)
{
  LOG(INFO) << "gRPC server: TfBuilderDisconnectionRequest";

  mConnManager.disconnectTfBuilder(*request, *response /*out*/);

  return Status::OK;
}


::grpc::Status TfSchedulerInstanceRpcImpl::TfBuilderUpdate(::grpc::ServerContext* /*context*/, const ::o2::DataDistribution::TfBuilderUpdateMessage* request, ::google::protobuf::Empty* /*response*/)
{
  // LOG(INFO) << "gRPC server: TfBuilderDisconnectionRequest: " <<  request->info().process_id();

  mTfBuilderInfo.updateTfBuilderInfo(*request);

  return Status::OK;
}


::grpc::Status TfSchedulerInstanceRpcImpl::StfSenderStfUpdate(::grpc::ServerContext* /*context*/, const ::o2::DataDistribution::StfSenderStfInfo* request, ::o2::DataDistribution::SchedulerStfInfoResponse* response)
{
  // LOG(INFO) << "gRPC server: StfSenderStfUpdate: " <<  request->info().process_id();
  response->Clear();
  mStfInfo.addAddStfInfo(*request, *response /*out*/);

  return Status::OK;
}






}
} /* o2::DataDistribution */
