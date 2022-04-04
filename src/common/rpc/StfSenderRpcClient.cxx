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

#include "StfSenderRpcClient.h"

#include <string>
#include <chrono>

namespace o2::DataDistribution
{

StfSenderRpcClient::StfSenderRpcClient(const std::string &pEndpoint)
{
  mChannel = grpc::CreateChannel(pEndpoint, grpc::InsecureChannelCredentials());
  mStub = StfSenderRpc::NewStub(mChannel);

  // trigger faster connection
  mChannel->GetState(true);
  mChannel->WaitForConnected(gpr_now(GPR_CLOCK_MONOTONIC));
}

bool StfSenderRpcClient::is_ready() const {
  // check state, the flag will move it from IDLE to READY
  mChannel->GetState(true);
  // check state and try to reconnect (keep alive)
  return (mChannel->GetState(true) == grpc_connectivity_state::GRPC_CHANNEL_READY);
}

std::string StfSenderRpcClient::grpc_status() {
  // check state, the flag will move it from IDLE to READY
  switch(mChannel->GetState(true)) {
    case grpc_connectivity_state::GRPC_CHANNEL_CONNECTING:
      return "GRPC_CHANNEL_CONNECTING";
    case grpc_connectivity_state::GRPC_CHANNEL_IDLE:
      return "GRPC_CHANNEL_IDLE";
    case grpc_connectivity_state::GRPC_CHANNEL_READY:
      return "GRPC_CHANNEL_READY";
    case grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN:
      return "GRPC_CHANNEL_SHUTDOWN";
    case grpc_connectivity_state::GRPC_CHANNEL_TRANSIENT_FAILURE:
      return "GRPC_CHANNEL_TRANSIENT_FAILURE";
    // default:


  }
  return "GRPC_CHANNEL_UNKNOWN_STATE";
}


}
