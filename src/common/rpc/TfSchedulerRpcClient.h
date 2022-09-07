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

#ifndef ALICEO2_DATADIST_TFSCHEDULER_RPC_CLIENT_H_
#define ALICEO2_DATADIST_TFSCHEDULER_RPC_CLIENT_H_

#include "ConfigConsul.h"

#include <discovery.pb.h>
#include <discovery.grpc.pb.h>
#include <grpcpp/grpcpp.h>

#include <vector>
#include <map>
#include <thread>

namespace o2::DataDistribution
{

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Status;


class TfSchedulerRpcClient {
public:
  TfSchedulerRpcClient() { }

  template <typename ConsulCli>
  bool start(std::shared_ptr<ConsulCli> pConfig) {

    if (!should_retry_start()) {
      return false;
    }

    const auto &lPartitionId = pConfig->status().partition().partition_id();

    using namespace std::chrono_literals;

    mTfSchedulerConf.Clear();
    if (!pConfig->getTfSchedulerConfig(lPartitionId, mTfSchedulerConf)) {
      // check for terminal states and exit
      if (mTfSchedulerConf.partition_state() == PartitionState::PARTITION_ERROR ||
          mTfSchedulerConf.partition_state() == PartitionState::PARTITION_TERMINATED) {
        WDDLOG_RL(10000, "Partition State: {}", PartitionState_Name(mTfSchedulerConf.partition_state()));
        mShouldRetryStart = false;
        return false;
      }

      IDDLOG_RL(5000, "TfScheduler instance configuration not found. Retrying.");
      return false;
    }

    // do not create a new connection if already connected
    if (mChannel) {
      return true;
    }

    const std::string &lEndpoint = mTfSchedulerConf.rpc_endpoint();

    mChannel = grpc::CreateChannel(lEndpoint, grpc::InsecureChannelCredentials());
    mStub = TfSchedulerInstanceRpc::NewStub(mChannel);

    // speed up connection
    mChannel->GetState(true);
    mChannel->WaitForConnected(gpr_now(GPR_CLOCK_MONOTONIC));

    IDDLOG("Connected to TfScheduler RPC endpoint={}", lEndpoint);

    mStarted = true;

    return true;
  }

  void stop() {
    mTfSchedulerConf.Clear();
    mStub.reset(nullptr);
    mChannel.reset();
    mStarted = false;
  }

  bool started() const { return mStarted; }

  void updateTimeInformation(BasicInfo &pInfo);

  // rpc HeartBeat(BasicInfo) returns (google.protobuf.Empty) { }
  bool HeartBeat(const BasicInfo &pInfo);

  // rpc NumStfSendersInPartitionRequest(google.protobuf.Empty) returns (NumStfSendersInPartitionResponse) { }
  bool NumStfSendersInPartitionRequest(std::uint32_t &pNumStfSenders);

  // TfBuilder FairMQ connect/disconnect
  // rpc TfBuilderConnectionRequest(TfBuilderConfigStatus) returns (TfBuilderConnectionResponse) { }
  bool TfBuilderConnectionRequest(TfBuilderConfigStatus &pParam, TfBuilderConnectionResponse &pRet /*out*/);

  // rpc TfBuilderDisconnectionRequest(TfBuilderConfigStatus) returns (StatusResponse) { }
  bool TfBuilderDisconnectionRequest(TfBuilderConfigStatus &pParam, StatusResponse &pRet /*out*/);

  // TfBuilder UCX connect/disconnect
  // rpc TfBuilderUCXConnectionRequest(TfBuilderConfigStatus) returns (TfBuilderUCXConnectionResponse) { }
  bool TfBuilderUCXConnectionRequest(TfBuilderConfigStatus &pParam, TfBuilderUCXConnectionResponse &pRet /*out*/);

  // rpc TfBuilderUCXDisconnectionRequest(TfBuilderConfigStatus) returns (StatusResponse) { }
  bool TfBuilderUCXDisconnectionRequest(TfBuilderConfigStatus &pParam,  StatusResponse &pRet /*out*/);


  // rpc TfBuilderUpdate(TfBuilderUpdateMessage) returns (google.protobuf.Empty) { }
  bool TfBuilderUpdate(TfBuilderUpdateMessage &pMsg);

  // rpc StfSenderStfUpdate(StfSenderStfInfo) returns (SchedulerStfInfoResponse) { }
  bool StfSenderStfUpdate(StfSenderStfInfo &pMsg, SchedulerStfInfoResponse &pRet);

  std::string getEndpoint() { return mTfSchedulerConf.rpc_endpoint(); }

  bool is_ready() const;
  bool is_alive() const {
    if (mChannel) {
      return (mChannel->GetState(true) != grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN);
    }
    return false;
  }

  bool should_retry_start() const { return mShouldRetryStart; }

private:
  TfSchedulerConfigStatus mTfSchedulerConf;

  std::unique_ptr<TfSchedulerInstanceRpc::Stub> mStub;
  std::shared_ptr<grpc::Channel> mChannel;

  // keep looking for the TfScheduler instance
  bool mShouldRetryStart = true;
  // mark successful start
  bool mStarted = false;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_TFSCHEDULER_RPC_CLIENT_H_ */
