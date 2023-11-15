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

#ifndef ALICEO2_DATADIST_TFBUILDER_RPC_CLIENT_H_
#define ALICEO2_DATADIST_TFBUILDER_RPC_CLIENT_H_

#include "ConfigConsul.h"

#include <discovery.pb.h>
#include <discovery.grpc.pb.h>
#include <grpcpp/grpcpp.h>

#include <vector>
#include <map>
#include <thread>
#include <shared_mutex>

namespace o2::DataDistribution
{

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ClientContext;
using grpc::Status;


class TfBuilderRpcClientCtx {
public:
  TfBuilderRpcClientCtx() { }
  ~TfBuilderRpcClientCtx() { stop(); }

  template<class T>
  bool start(std::shared_ptr<T> pConfig, const std::string &pTfBuilderId) {
    using namespace std::chrono_literals;
    if (mRunning) {
      return true;
    }

    const auto &lPartitionId = pConfig->status().partition().partition_id();
    assert (!lPartitionId.empty());

    mTfBuilderConf.Clear();
    if (!pConfig->getTfBuilderConfig(lPartitionId, pTfBuilderId, mTfBuilderConf)) {
      IDDLOG_RL(2000, "TfBuilder information is not discoverable. tfb_id={}", pTfBuilderId);
      return false;
    }

    const std::string &lEndpoint = mTfBuilderConf.rpc_endpoint();

    mChannel = grpc::CreateChannel(lEndpoint, grpc::InsecureChannelCredentials());
    mStub = TfBuilderRpc::NewStub(mChannel);

    // speed up connecting
    mChannel->GetState(true);
    mChannel->WaitForConnected(gpr_now(GPR_CLOCK_MONOTONIC));

    mRunning = true;

    if (is_alive()) {
      IDDLOG("Connected gRPC client to TfBuilder. tf_builder={:s} endpoint={:s}", pTfBuilderId, lEndpoint);
    } else {
      EDDLOG("Error in gRPC client to TfBuilder. tf_builder={:s} endpoint={:s}", pTfBuilderId, lEndpoint);
      return false;
    }
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
    auto lStart = std::chrono::steady_clock::now();

    ClientContext lContext;
    lContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(5000));
    pResponse.Clear();

    auto lStatus = mStub->BuildTfRequest(&lContext, pTfInfo, &pResponse);

    if (mMonitorDuration) {
      DDMON("datadist.grpc", "BuildTfRequest_ms", since<std::chrono::milliseconds>(lStart));
    }

    if (lStatus.ok()) {
      return true;
    }

    EDDLOG_RL(1000, "gRPC request error. code={} message={}", (int)lStatus.error_code(), lStatus.error_message());

    return false;
  }

  //  rpc TerminatePartition(PartitionInfo) returns (PartitionResponse) { }
  bool TerminatePartition(const PartitionInfo &pPartitionInfo) {
    ClientContext lContext;
    // we don't care about the success. TfBuilder will be terminated by ECS
    lContext.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));

    PartitionResponse lResponse;
    auto lStatus = mStub->TerminatePartition(&lContext, pPartitionInfo, &lResponse);

    // this is best effort only. ECS could have already stopped them
    DDDLOG("TerminatePartition: TfBuilder. tfb_id={} state={} message={}",
      mTfBuilderConf.info().process_id(), PartitionState_Name(lResponse.partition_state()), lStatus.error_message());
    return true;
  }

  std::string getEndpoint() { return mTfBuilderConf.rpc_endpoint(); }

  bool is_ready() const {
    if (mChannel) {
      return (mChannel->GetState(true) == grpc_connectivity_state::GRPC_CHANNEL_READY);
    }
    return false;
  }
  bool is_alive() const {
    if (mRunning && mChannel) {
      return (mChannel->GetState(true) != grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN);
    }
    return false;
  }

  void setMonitorDuration(const bool pMonitor) { mMonitorDuration = pMonitor; };

private:
  std::atomic_bool mRunning = false;
  // monitoring
  bool mMonitorDuration = false;

  TfBuilderConfigStatus mTfBuilderConf;

  std::unique_ptr<TfBuilderRpc::Stub> mStub;
  std::shared_ptr<grpc::Channel> mChannel;
};


class TfBuilderRpcClient {
public:
  TfBuilderRpcClient(TfBuilderRpcClientCtx *pCtx, std::recursive_mutex *pMtx)
  : mCliCtx(pCtx),
    mMtx(pMtx)
  {
    if (mMtx) {
      mMtx->lock();
    }
  }

  ~TfBuilderRpcClient() { put(); }

  TfBuilderRpcClientCtx& get() { return *mCliCtx; }
  void put()
  {
    if (mMtx) {
      mMtx->unlock();
      mCliCtx = nullptr;
      mMtx = nullptr;
    }
  }

  operator bool() const { return mCliCtx != nullptr; }

private:
  TfBuilderRpcClientCtx *mCliCtx;
  std::recursive_mutex *mMtx;
};


template <class T>
class TfBuilderRpcClientCollection {
public:
  TfBuilderRpcClientCollection(std::shared_ptr<T> pDiscoveryConfig)
  : mDiscoveryConfig(pDiscoveryConfig)
  { }

  struct RpcClient {
    std::unique_ptr<std::recursive_mutex>  mClientLock;
    std::unique_ptr<TfBuilderRpcClientCtx> mClient;
  };


  bool remove(const std::string pId)
  {
    std::unique_lock lLock(mClientsGlobalLock);
    if (mClients.count(pId) > 0) {
      RpcClient lCliStruct = std::move(mClients.at(pId));
      {
        // we have to wait for RpcClient lock before erasing
        std::scoped_lock lCliLock(*(lCliStruct.mClientLock));
        lCliStruct.mClient->stop();
        mClients.erase(pId);
      } // need to unlock lCliStruct.mClientLock here before destroying the object
      return true;
    }
    return false;
  }


  bool add(const std::string &pId)
  {
    std::unique_lock lLock(mClientsGlobalLock);
    remove(pId);
    mClients.emplace(pId, RpcClient());
    RpcClient &lCli = mClients[pId];
    lLock.unlock();

    lCli.mClientLock = std::make_unique<std::recursive_mutex>();
    lCli.mClient = std::make_unique<TfBuilderRpcClientCtx>();
    lCli.mClient->setMonitorDuration(mMonitorRpcDuration);

    auto lRet = lCli.mClient->start(mDiscoveryConfig, pId);
    if (!lRet) {
      std::unique_lock lLockErase(mClientsGlobalLock);
      mClients.erase(pId);
    }

    return lRet;
  }

  bool add_if_new(const std::string &pId)
  {
    std::unique_lock lLock(mClientsGlobalLock);

    if (mClients.count(pId) == 0) {
      mClients.emplace(pId, RpcClient());
      RpcClient &lCli = mClients[pId];
      lLock.unlock();

      lCli.mClientLock = std::make_unique<std::recursive_mutex>();
      lCli.mClient = std::make_unique<TfBuilderRpcClientCtx>();
      lCli.mClient->setMonitorDuration(mMonitorRpcDuration);

      auto lRet = lCli.mClient->start(mDiscoveryConfig, pId);
      if (!lRet) {
        std::unique_lock lLockErase(mClientsGlobalLock);
        mClients.erase(pId);
      }
    }

    std::shared_lock lLockReturn(mClientsGlobalLock);
    return mClients[pId].mClient->is_alive();
  }

  TfBuilderRpcClient get(const std::string &pId)
  {
    std::shared_lock lLock(mClientsGlobalLock);

    if (mClients.count(pId) > 0) {

      RpcClient &lCli = mClients[pId];

      return TfBuilderRpcClient(lCli.mClient.get(), lCli.mClientLock.get());
    }

    return TfBuilderRpcClient(nullptr, nullptr);
  }

  std::size_t size() const { return mClients.size(); }
  std::size_t count(const std::string &pId) const { return mClients.count(pId); }
  // auto& operator[] const (const std::string &pId) const { return mClients.at(pId); }

  auto begin() const { return mClients.begin(); }
  auto end() const { return mClients.end(); }

  void clear() {
    // remove each of the clients while holding their locks
    std::unique_lock lLock(mClientsGlobalLock);

    for (auto &[lId, lCli] : mClients) {
      (void)lCli;
      RpcClient lCliStruct = std::move(mClients.at(lId));

      // we have to wait for RpcClient lock before erasing
      std::scoped_lock lCliLock(*(lCliStruct.mClientLock));
      lCliStruct.mClient->stop();
    }

    mClients.clear();
  }

  void setMonitorDuration(const bool pMon) {
    mMonitorRpcDuration = pMon;
    std::shared_lock lLock(mClientsGlobalLock);
    for (auto &[ mCliId, lClient] : mClients) {
      (void) mCliId;
      lClient.mClient->setMonitorDuration(mMonitorRpcDuration);
    }
  }

private:

  std::shared_ptr<T> mDiscoveryConfig;

  std::shared_mutex mClientsGlobalLock;
  std::map<std::string, RpcClient> mClients;

  // monitoring
  bool mMonitorRpcDuration = false;
};


} /* namespace o2::DataDistribution */

#endif /* ALICEO2_DATADIST_TFBUILDER_RPC_CLIENT_H_ */
