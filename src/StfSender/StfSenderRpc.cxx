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

#include "StfSenderRpc.h"
#include "StfSenderOutput.h"
#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <stdexcept>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;



void StfSenderRpcImpl::start(const std::string pRpcSrvBindIp, int &lRealPort /*[out]*/)
{
  ServerBuilder lSrvBuilder;
  lSrvBuilder.AddListeningPort(pRpcSrvBindIp + ":0", grpc::InsecureServerCredentials(), &lRealPort);
  lSrvBuilder.RegisterService(this);

  assert(!mServer);
  mServer = lSrvBuilder.BuildAndStart();

  DDLOG(fair::Severity::INFO) << "gRPC server listening on : " << pRpcSrvBindIp << ":" << lRealPort;
}

void StfSenderRpcImpl::stop()
{
  if (mServer) {
    mServer->Shutdown();
    mServer.reset(nullptr);
  }
}

::grpc::Status StfSenderRpcImpl::ConnectTfBuilderRequest(::grpc::ServerContext* /*context*/,
                                          const TfBuilderEndpoint* request,
                                          ConnectTfBuilderResponse* response)
{
  const std::string lTfSenderId = request->tf_builder_id();
  const std::string lTfSenderEndpoint = request->endpoint();

  // handle the request
  DDLOG(fair::Severity::INFO) << "Requested to connect to TfBuilder " << lTfSenderId << " at endpoint: " << lTfSenderEndpoint;
  response->set_status(OK);

  const auto lStatus = mOutput.connectTfBuilder(lTfSenderId, lTfSenderEndpoint);
  switch (lStatus) {
    case StfSenderOutput::ConnectStatus::eOK:
      response->set_status(OK);
      break;
    case StfSenderOutput::ConnectStatus::eCONNERR:
      response->set_status(ERROR_STF_SENDER_CONNECTING);
      break;
    case StfSenderOutput::ConnectStatus::eEXISTS:
      response->set_status(ERROR_STF_SENDER_EXISTS);
      break;
  }

  return Status::OK;
}

::grpc::Status StfSenderRpcImpl::DisconnectTfBuilderRequest(::grpc::ServerContext* /*context*/,
                                          const TfBuilderEndpoint* request,
                                          StatusResponse* response)
{

  const std::string lTfSenderId = request->tf_builder_id();
  const std::string lTfSenderEndpoint = request->endpoint();

  // handle the request
  DDLOG(fair::Severity::INFO) << "Requested to disconnect TfBuilder " << lTfSenderId << " at endpoint: " << lTfSenderEndpoint;
  response->set_status(0);

  if (!mOutput.disconnectTfBuilder(lTfSenderId, lTfSenderEndpoint)) {
    response->set_status(-1);
  }

  return Status::OK;
}

::grpc::Status StfSenderRpcImpl::StfDataRequest(::grpc::ServerContext* /*context*/,
                                const StfDataRequestMessage* request,
                                StfDataResponse* response)
{

  mOutput.sendStfToTfBuilder(request->stf_id(), request->tf_builder_id(), *response/*out*/);

  return Status::OK;
}


}
} /* o2::DataDistribution */
