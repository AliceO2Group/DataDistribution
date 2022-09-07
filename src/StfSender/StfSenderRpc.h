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

namespace o2::DataDistribution
{
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class StfSenderOutput;

class StfSenderRpcImpl final : public StfSenderRpc::Service
{
 public:
  StfSenderRpcImpl(const std::string &pPartitionId)
  : mPartitionId(pPartitionId),
    mServer(nullptr)
  { }

  virtual ~StfSenderRpcImpl() { }

  // FairMQ channels
  // rpc ConnectTfBuilderRequest(TfBuilderEndpoint) returns (ConnectTfBuilderResponse) { }
  ::grpc::Status ConnectTfBuilderRequest(::grpc::ServerContext* context,
                                          const TfBuilderEndpoint* request,
                                          ConnectTfBuilderResponse* response) override;

  // rpc DisconnectTfBuilderRequest(TfBuilderEndpoint) returns (StatusResponse) { }
  ::grpc::Status DisconnectTfBuilderRequest(::grpc::ServerContext* context,
                                            const TfBuilderEndpoint* request,
                                            StatusResponse* response) override;

  // UCX channels
  // rpc ConnectTfBuilderUCXRequest(TfBuilderUCXEndpoint) returns (ConnectTfBuilderUCXResponse) { }
  ::grpc::Status ConnectTfBuilderUCXRequest(::grpc::ServerContext* context,
                                            const TfBuilderUCXEndpoint* request,
                                            ConnectTfBuilderUCXResponse* response) override;

  // rpc DisconnectTfBuilderUCXRequest(TfBuilderUCXEndpoint) returns (StatusResponse) { }
  ::grpc::Status DisconnectTfBuilderUCXRequest(::grpc::ServerContext* context,
                                                const TfBuilderUCXEndpoint* request,
                                                StatusResponse* response) override;

  // rpc StfDataRequest(StfDataRequestMessage) returns (StfDataResponse) { }
  ::grpc::Status StfDataRequest(::grpc::ServerContext* context,
                                const StfDataRequestMessage* request,
                                StfDataResponse* response) override;

  // rpc TerminatePartition(PartitionInfo) returns (PartitionResponse) { }
  ::grpc::Status TerminatePartition(::grpc::ServerContext* context,
                                    const PartitionInfo* request,
                                    PartitionResponse* response) override;

  void start(StfSenderOutput *pOutput, const std::string pRpcSrvBindIp, int& lRealPort /*[out]*/);
  void stop();

  bool isTerminateRequested() const { return mTerminateRequested; }

 private:
  std::string mPartitionId;
  bool mTerminateRequested = false;
  std::unique_ptr<Server> mServer = nullptr;
  StfSenderOutput *mOutput = nullptr;

};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STF_SENDER_RPC_H_ */
