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

#ifndef ALICEO2_STF_SENDER_RPC_H_
#define ALICEO2_STF_SENDER_RPC_H_

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <grpcpp/grpcpp.h>
#include <discovery.grpc.pb.h>
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
using grpc::Status;

class StfSenderOutput;

class StfSenderRpcImpl final : public StfSenderRpc::Service
{
 public:
  StfSenderRpcImpl()
  :
  mServer(nullptr)
  { }


  virtual ~StfSenderRpcImpl() { }

  // rpc ConnectTfBuilderRequest(TfBuilderEndpoint) returns (ConnectTfBuilderResponse) { }
  ::grpc::Status ConnectTfBuilderRequest(::grpc::ServerContext* context,
                                          const TfBuilderEndpoint* request,
                                          ConnectTfBuilderResponse* response) override;

  // rpc DisconnectTfBuilderRequest(TfBuilderEndpoint) returns (StatusResponse) { }
  ::grpc::Status DisconnectTfBuilderRequest(::grpc::ServerContext* context,
                                          const TfBuilderEndpoint* request,
                                          StatusResponse* response) override;

  // rpc StfDataRequest(StfDataRequestMessage) returns (StfDataResponse) { }
  ::grpc::Status StfDataRequest(::grpc::ServerContext* context,
                                const StfDataRequestMessage* request,
                                StfDataResponse* response) override;

  void start(StfSenderOutput *pOutput, const std::string pRpcSrvBindIp, int& lRealPort /*[out]*/);
  void stop();

 private:
  std::unique_ptr<Server> mServer = nullptr;
  StfSenderOutput *mOutput = nullptr;

};
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STF_SENDER_RPC_H_ */
