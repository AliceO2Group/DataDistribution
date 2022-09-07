// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

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
