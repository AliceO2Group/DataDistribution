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

#ifndef ALICEO2_TF_SCHEDULER_INSTANCE_RPC_H_
#define ALICEO2_TF_SCHEDULER_INSTANCE_RPC_H_

#include "TfSchedulerConnManager.h"
#include "TfSchedulerTfBuilderInfo.h"
#include "TfSchedulerStfInfo.h"
#include "TfSchedulerTokenManager.h"

#include <ConfigConsul.h>
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

class TfSchedulerInstanceRpcImpl final : public TfSchedulerInstanceRpc::Service
{
 public:
  TfSchedulerInstanceRpcImpl() = delete;
  TfSchedulerInstanceRpcImpl(std::shared_ptr<ConsulTfScheduler> pDiscoveryConfig, const PartitionRequest &pPartitionRequest)
  :
  mDiscoveryConfig(pDiscoveryConfig),
  mPartitionInfo(pPartitionRequest),
  mConnManager(pDiscoveryConfig, pPartitionRequest),
  mTfBuilderInfo(pDiscoveryConfig),
  mStfInfo(pDiscoveryConfig, mConnManager, mTfBuilderInfo),
  mTokenManager(pDiscoveryConfig)
  { }

  virtual ~TfSchedulerInstanceRpcImpl() { }

  ::grpc::Status HeartBeat(::grpc::ServerContext* context, const BasicInfo* request, ::google::protobuf::Empty* response) override;

  ::grpc::Status GetPartitionState(::grpc::ServerContext* context, const PartitionInfo* request, PartitionResponse* response) override;
  ::grpc::Status TerminatePartition(::grpc::ServerContext* context, const PartitionInfo* request, PartitionResponse* response) override;

  ::grpc::Status NumStfSendersInPartitionRequest(::grpc::ServerContext* context, const ::google::protobuf::Empty* request, NumStfSendersInPartitionResponse* response) override;

  // TfBuilder FairMQ connect/disconnect
  ::grpc::Status TfBuilderConnectionRequest(::grpc::ServerContext* context, const TfBuilderConfigStatus* request, TfBuilderConnectionResponse* response) override;
  ::grpc::Status TfBuilderDisconnectionRequest(::grpc::ServerContext* context, const TfBuilderConfigStatus* request, StatusResponse* response) override;

  // TfBuilder UCX connect/disconnect
  ::grpc::Status TfBuilderUCXConnectionRequest(::grpc::ServerContext* context, const TfBuilderConfigStatus* request, TfBuilderUCXConnectionResponse* response) override;
  ::grpc::Status TfBuilderUCXDisconnectionRequest(::grpc::ServerContext* context, const TfBuilderConfigStatus* request, StatusResponse* response) override;

  ::grpc::Status TfBuilderUpdate(::grpc::ServerContext* context, const TfBuilderUpdateMessage* request, ::google::protobuf::Empty* response) override;
  ::grpc::Status StfSenderStfUpdate(::grpc::ServerContext* context, const StfSenderStfInfo* request, SchedulerStfInfoResponse* response) override;


  void initDiscovery(const std::string pRpcSrvBindIp, int &lRealPort /*[out]*/);
  bool start();
  void stop();

  void PartitionMonitorThread();

  bool accepting_updates() const {
    return !(
      mPartitionState == PartitionState::PARTITION_TERMINATING ||
      mPartitionState == PartitionState::PARTITION_TERMINATED ||
      mPartitionState == PartitionState::PARTITION_ERROR
    );
  }

  PartitionState getPartitionState() const { return mPartitionState; }

 private:
  /// Partition monitoring thread
  std::atomic_bool mRunning = false;
  std::thread mMonitorThread;

  /// Discovery
  std::shared_ptr<ConsulTfScheduler> mDiscoveryConfig;
  /// Partition information
  PartitionRequest mPartitionInfo;

  /// gRPC server object
  std::unique_ptr<Server> mServer;

  /// Connection manager between StfSenders and TfBuilders + RPC clients
  TfSchedulerConnManager mConnManager;

  /// TfBuilder status database
  TfSchedulerTfBuilderInfo mTfBuilderInfo;

  /// Stfs for scheduling
  TfSchedulerStfInfo mStfInfo;

  /// Stf schedule token maker
  TfSchedulerTokenManager mTokenManager;
  bool mTokenManagerEnabled = DataDistEnableStfTransferTokensDefault;

  /// Partition State. Always update via the method.
  void updatePartitionState(const PartitionState pNewState);
  PartitionState mPartitionState = PartitionState::PARTITION_CONFIGURING;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_SCHEDULER_INSTANCE_RPC_H_ */
