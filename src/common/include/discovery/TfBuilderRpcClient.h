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

#ifndef ALICEO2_DATADIST_TFBUILDER_RPC_CLIENT_H_
#define ALICEO2_DATADIST_TFBUILDER_RPC_CLIENT_H_

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


class TfBuilderRpcClient {
public:
  TfBuilderRpcClient() { }
  ~TfBuilderRpcClient() { stop(); }

  template<class T>
  bool start(std::shared_ptr<T> pConfig, const std::string &pTfBuilderId) {
    using namespace std::chrono_literals;

    const auto &lPartitionId = pConfig->status().partition().partition_id();
    assert (!lPartitionId.empty());

    mTfBuilderConf.Clear();
    if (!pConfig->getTfBuilderConfig(lPartitionId, pTfBuilderId, mTfBuilderConf)) {
      LOG(WARNING) << "TfBuilder information is not discoverable";
      return false;
    }

    const std::string &lEndpoint = mTfBuilderConf.rpc_endpoint();

    std::this_thread::sleep_for(2s);

    mStub = std::move(TfBuilderRpc::NewStub(
      grpc::CreateChannel(lEndpoint, grpc::InsecureChannelCredentials()))
    );

    LOG(INFO) << "Connected gRPC client to TfBuilder: " << pTfBuilderId
              << ", endpoint: " << lEndpoint;

    mRunning = true;
    return true;
  }

  void stop() {
    mRunning = false;
    mTfBuilderConf.Clear();
    mStub.reset(nullptr);
  }

  void updateTimeInformation(BasicInfo &pInfo) {
    auto [lTimeStr, lTimet] = ConsulImpl::getCurrentTimeString();
    pInfo.set_last_update(lTimeStr);
    pInfo.set_last_update_t(lTimet);
  }

  // rpc BuildTfRequest(TfBuildingInformation) returns (BuildTfResponse) { }

  bool BuildTfRequest(const TfBuildingInformation &pTfInfo, BuildTfResponse &pResponse /*out */)
  {
    using namespace std::chrono_literals;

    ClientContext lContext;
    pResponse.Clear();

    auto lStatus = mStub->BuildTfRequest(&lContext, pTfInfo, &pResponse);
    if (lStatus.ok()) {
      return true;
    }

    LOG(ERROR) << "gRPC request error code: " << lStatus.error_code() << " message: " << lStatus.error_message();

    return false;
  }


  std::string getEndpoint() { return mTfBuilderConf.rpc_endpoint(); }


private:
  std::atomic_bool mRunning = false;

  TfBuilderConfigStatus mTfBuilderConf;

  std::unique_ptr<TfBuilderRpc::Stub> mStub;
};


}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_TFBUILDER_RPC_CLIENT_H_ */
