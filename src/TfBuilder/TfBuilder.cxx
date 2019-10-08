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

#include "TfBuilderRpc.h"
#include <grpcpp/grpcpp.h>

#include <FairMQLogger.h>

#include <condition_variable>
#include <stdexcept>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

::grpc::Status
TfBuilderRpcImpl::ConnectTfBuilderRequest(::grpc::ServerContext* /*context*/,
                                          const ::o2::DataDistribution::TfBuilderEndpoint* request,
                                          ::o2::DataDistribution::StatusResponse* response)
{

  const std::string lTfSenderId = request->tf_builder_id();
  const std::string lTfSenderEndpoint = request->endpoint();


  // handle the request
  LOG(INFO) << "Requested to connect to TfBuilder " << lTfSenderId << " at endpoint: " << lTfSenderEndpoint;

  response->set_status(0);

  return Status::OK;
}

void TfBuilderRpcImpl::start(const std::string pRpcSrvBindIp, int &lRealPort /*[out]*/)
{
  ServerBuilder lSrvBuilder;
  lSrvBuilder.AddListeningPort(pRpcSrvBindIp + ":0", grpc::InsecureServerCredentials(), &lRealPort);
  lSrvBuilder.RegisterService(this);

  assert(!mServer);
  mServer = std::move(lSrvBuilder.BuildAndStart());

  LOG(INFO) << "gRPC server listening on : " << pRpcSrvBindIp << ":" << lRealPort;
}

void TfBuilderRpcImpl::stop()
{
  if (mServer) {
    mServer->Shutdown();
    mServer.reset(nullptr);
  }
}


}
} /* o2::DataDistribution */
