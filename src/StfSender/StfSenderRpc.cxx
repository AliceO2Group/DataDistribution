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

#include "StfSenderRpc.h"
#include "StfSenderOutputDefs.h"
#include "StfSenderOutput.h"
#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <stdexcept>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

void StfSenderRpcImpl::start(StfSenderOutput *pOutput, const std::string pRpcSrvBindIp, int& lRealPort /*[out]*/)
{
  mOutput = pOutput;

  IDDLOG("Starting the gRPC server... ip={}", pRpcSrvBindIp);

  ServerBuilder lSrvBuilder;
  lSrvBuilder.SetSyncServerOption(ServerBuilder::SyncServerOption::NUM_CQS, 4);
  lSrvBuilder.SetSyncServerOption(ServerBuilder::SyncServerOption::MAX_POLLERS, 20);
  lSrvBuilder.AddListeningPort(pRpcSrvBindIp + ":0", grpc::InsecureServerCredentials(), &lRealPort);
  lSrvBuilder.RegisterService(this);

  assert(!mServer);
  mServer = lSrvBuilder.BuildAndStart();

  IDDLOG("gRPC server listening on {}:{}", pRpcSrvBindIp, lRealPort);
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
  const std::string lTfBuilderId = request->tf_builder_id();
  const std::string lTfBuilderEndpoint = request->endpoint();

  if (mTerminateRequested) {
    response->set_status(TfBuilderConnectionStatus::ERROR_PARTITION_TERMINATING);
    return Status::OK;
  }

  // handle the request
  DDDLOG("Requested to connect to TfBuilder. tfb_id={} tfb_ep={}", lTfBuilderId, lTfBuilderEndpoint);
  response->set_status(OK);

  const auto lStatus = mOutput->connectTfBuilder(lTfBuilderId, lTfBuilderEndpoint);
  switch (lStatus) {
    case ConnectStatus::eOK:
      response->set_status(OK);
      break;
    case ConnectStatus::eCONNERR:
      response->set_status(ERROR_STF_SENDER_CONNECTING);
      break;
    case ConnectStatus::eEXISTS:
      response->set_status(ERROR_STF_SENDER_EXISTS);
      break;
  }

  return Status::OK;
}

::grpc::Status StfSenderRpcImpl::DisconnectTfBuilderRequest(::grpc::ServerContext* /*context*/,
                                          const TfBuilderEndpoint* request,
                                          StatusResponse* response)
{
  const std::string lTfBuilderId = request->tf_builder_id();
  const std::string lTfBuilderEndpoint = request->endpoint();

  // handle the request
  DDDLOG("Requested to disconnect from TfBuilder. tfb_id={} tfb_ep={}", lTfBuilderId, lTfBuilderEndpoint);
  response->set_status(0);

  if (!mOutput->disconnectTfBuilder(lTfBuilderId, lTfBuilderEndpoint)) {
    response->set_status(-1);
  }

  return Status::OK;
}

::grpc::Status StfSenderRpcImpl::StfDataRequest(::grpc::ServerContext* /*context*/,
                                const StfDataRequestMessage* request,
                                StfDataResponse* response)
{
  mOutput->sendStfToTfBuilder(request->stf_id(), request->tf_builder_id(), *response/*out*/);
  return Status::OK;
}

::grpc::Status StfSenderRpcImpl::ConnectTfBuilderUCXRequest(::grpc::ServerContext* /*context*/,
                                                            const TfBuilderUCXEndpoint* request,
                                                            ConnectTfBuilderUCXResponse* response)
{
  const std::string &lTfBuilderId = request->tf_builder_id();
  const auto &lTfBuilderEp = request->endpoint();

  if (mTerminateRequested) {
    response->set_status(TfBuilderConnectionStatus::ERROR_PARTITION_TERMINATING);
    return Status::OK;
  }

  // handle the request
  DDDLOG("Requested to connect to UCX TfBuilder. tfb_id={} tfb_ip={} tfb_port={}",
    lTfBuilderId, lTfBuilderEp.listen_ep().ip(), lTfBuilderEp.listen_ep().port());
  response->set_status(OK);

  const auto lStatus = mOutput->connectTfBuilderUCX(lTfBuilderId, lTfBuilderEp.listen_ep().ip(), lTfBuilderEp.listen_ep().port());
  switch (lStatus) {
    case ConnectStatus::eOK:
      response->set_status(OK);
      break;
    case ConnectStatus::eCONNERR:
      response->set_status(ERROR_STF_SENDER_CONNECTING);
      break;
    case ConnectStatus::eEXISTS:
      response->set_status(ERROR_STF_SENDER_EXISTS);
      break;
  }

  return Status::OK;
}

::grpc::Status StfSenderRpcImpl::DisconnectTfBuilderUCXRequest(::grpc::ServerContext* /*context*/,
                                                                const TfBuilderUCXEndpoint* request,
                                                                StatusResponse* response)
{
  const std::string &lTfBuilderId = request->tf_builder_id();
  const auto &lTfBuilderEp = request->endpoint();

  // handle the request
  DDDLOG("Requested to disconnect from UCX TfBuilder. tfb_id={} tfb_ip={} tfb_port={}",
    lTfBuilderId, lTfBuilderEp.listen_ep().ip(), lTfBuilderEp.listen_ep().port());
  response->set_status(0);

  if (!mOutput->disconnectTfBuilderUCX(lTfBuilderId)) {
    response->set_status(-1);
  }
  return Status::OK;
}

// rpc TerminatePartition(PartitionInfo) returns (PartitionResponse) { }
::grpc::Status StfSenderRpcImpl::TerminatePartition(::grpc::ServerContext* /*context*/,
  const PartitionInfo* request, PartitionResponse* response)
{
  if (request->partition_id() != mPartitionId) {
    response->set_partition_state(PartitionState::PARTITION_UNKNOWN);
    return Status::CANCELLED;
  }

  IDDLOG_GRL(5000, "TerminatePartition request received. partition_id={}", request->partition_id());

  mTerminateRequested = true;
  return Status::OK;
}

} /* o2::DataDistribution */
