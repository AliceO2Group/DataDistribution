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

#ifndef ALICEO2_TF_BUILDER_RPC_H_
#define ALICEO2_TF_BUILDER_RPC_H_

#include "TfBuilderInputDefs.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <grpcpp/grpcpp.h>
#include <discovery.grpc.pb.h>
#pragma GCC diagnostic pop

#include <ConfigConsul.h>
#include <TfSchedulerRpcClient.h>
#include <StfSenderRpcClient.h>
#include <SubTimeFrameDataModel.h>

#include <ConcurrentQueue.h>
#include <DataDistributionOptions.h>

#include <vector>
#include <deque>
#include <map>
#include <thread>
#include <mutex>

namespace o2::DataDistribution
{
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class SyncMemoryResources;

class TfBuilderRpcImpl final : public TfBuilderRpc::Service
{
public:
  TfBuilderRpcImpl(std::shared_ptr<ConsulTfBuilder> pDiscoveryConfig, SyncMemoryResources &pMemI)
  : mMemI(pMemI),
    mDiscoveryConfig(pDiscoveryConfig),
    mStfSenderRpcClients(mDiscoveryConfig)
  { }

  virtual ~TfBuilderRpcImpl() { }

  TfSchedulerRpcClient& TfSchedRpcCli() { return mTfSchedulerRpcClient; }

  void initDiscovery(const std::string pRpcSrvBindIp, int &lRealPort /*[out]*/);
  bool start(const std::uint64_t pBufferSize, std::shared_ptr<ConcurrentQueue<std::string> > pReqQueue,
    std::shared_ptr<ConcurrentQueue<ReceivedStfMeta> > pRecvQueue);
  void stop();

  unsigned getNumFailedRpcConnections() const {
    return (mStfSenderRpcClients.getNumConnectedClients() - mStfSenderRpcClients.getNumWorkingClients());
  }

  std::vector<std::string> getStfSenderIds() const { return mStfSenderRpcClients.getStfSenderIds(); }

  void startAcceptingTfs();
  void stopAcceptingTfs();

  void UpdateSendingThread();
  void StfRequestThread();
  void StfRequestGrpcThread();

  bool recordStfReceived(const std::string &pStfSenderId, const std::uint64_t pTfId);
  bool recordTfBuilt(const SubTimeFrame &pTf);
  bool recordTfForwarded(const std::uint64_t &pTfId);
  bool sendTfBuilderUpdate();

  bool getNewTfBuildingRequest(TfBuildingInformation &pNewTfRequest)
  { if (!mTfBuildRequests) {
      return false;
    }
    return mTfBuildRequests->pop(pNewTfRequest);
  }

  StfSenderRpcClientCollection<ConsulTfBuilder>& StfSenderRpcClients() { return mStfSenderRpcClients; }

  bool isTerminateRequested() const { return mTerminateRequested; }

  // rpc BuildTfRequest(TfBuildingInformation) returns (BuildTfResponse) { }
  ::grpc::Status BuildTfRequest(::grpc::ServerContext* context, const TfBuildingInformation* request, BuildTfResponse* response) override;

  ::grpc::Status TerminatePartition(::grpc::ServerContext* context, const ::o2::DataDistribution::PartitionInfo* request, ::o2::DataDistribution::PartitionResponse* response) override;

  /// reset counters on each new run
  void reset_run_counters() {
    std::scoped_lock lLock(mTfIdSizesLock, mTopoTfIdLock);

    mTfIdSizes.clear();
    mCurrentTfBufferSize = mBufferSize;
    mLastBuiltTfId = 0;
    mNumBufferedTfs = 0;
    mNumTfsInBuilding = 0;
    mNumReqInFlight = 0;
    if (mTfBuildRequests) {
      mTfBuildRequests->flush();
    }

    // Reset Topo Tf Id renaming
    mTopoStfId = 1;
    mTopoTfIdRenameMap.clear();
  }

  std::optional<std::uint64_t> getNumberOfStfs(const TimeFrameIdType pTfId) {
    std::scoped_lock lLock(mStfsCountMapLock);
    if (mStfsCountMap.count(pTfId) == 0) {
      return std::nullopt;
    }
    return mStfsCountMap[pTfId];
  }
  void setNumberOfStfs(const TimeFrameIdType pTfId, const std::optional<std::uint64_t> &pNumStfsOpt) {
    std::scoped_lock lLock(mStfsCountMapLock);
    if (pNumStfsOpt == std::nullopt) {
      mStfsCountMap.erase(pTfId);
    } else {
      mStfsCountMap[pTfId] = pNumStfsOpt.value();
    }
  }

  std::uint64_t getIdForTopoTf(const std::string &pStfSenderId, const std::uint64_t pTfId) {
    std::scoped_lock lLock(mTopoTfIdLock);

    assert (mTopoTfIdRenameMap.count(pStfSenderId) == 1);
    assert (mTopoTfIdRenameMap[pStfSenderId].count(pTfId) == 1);

    const auto lRet = mTopoTfIdRenameMap[pStfSenderId][pTfId];
    mTopoTfIdRenameMap[pStfSenderId].erase(pTfId);

    assert (lRet > 0);
    return lRet;
  }

private:
  std::atomic_bool mRunning = false;
  std::atomic_bool mTerminateRequested = false;

  std::atomic_bool mAcceptingTfs = false;
  std::mutex mUpdateLock;

  /// Update sending thread
  std::condition_variable mUpdateCondition;
  std::thread mUpdateThread;

    struct StfRequest {
      std::string mStfSenderId;
      std::uint64_t mStfDataSize;
      StfDataRequestMessage mRequest;

      StfRequest() = default;
      StfRequest(const std::string &pStfSenderId, const std::uint64_t pStfDataSize, const StfDataRequestMessage &pRequest)
      : mStfSenderId(pStfSenderId), mStfDataSize(pStfDataSize), mRequest(pRequest) { }
    };

    enum StfRequestIdxSel {
      eRandom = 0,
      eLinear,
      eStfSize
    };

    static constexpr std::string_view sStfRequestIdxSelNames[] = {
      "random",
      "linear",
      "stfsize"
    };

    std::atomic<StfRequestIdxSel> mStfSenderIdxSelMethod = eRandom;

    std::size_t getFetchIdxStfDataSize(const std::vector<StfRequest> &pReqVector) const;
    std::size_t getFetchIdxRandom(const std::vector<StfRequest> &pReqVector) const;
    std::size_t getFetchIdxLinear(const std::vector<StfRequest> &pReqVector) const { return (pReqVector.size() - 1); }

    std::size_t getFetchIdx(const std::vector<StfRequest> &pReqVector) {
      assert (pReqVector.size() > 0);

      switch (mStfSenderIdxSelMethod.load()) {
        case eRandom:
          return getFetchIdxRandom(pReqVector);
        case eLinear:
          return getFetchIdxLinear(pReqVector);
        case eStfSize:
          return getFetchIdxStfDataSize(pReqVector);
      };

      return 0;
    };

    void UpdateConsulParams();


  std::mutex mNumInFlightLock;
    std::condition_variable mNumInFlightCond;
    std::uint64_t mMaxNumReqInFlight = MaxNumStfTransferDefault;
    std::uint64_t mNumReqInFlight = 0;

  // <tfid, topo?, topo_id, stf_requests>
  ConcurrentFifo<std::tuple<std::uint64_t, bool, std::uint64_t, std::vector<StfRequest>> >  mStfRequestQueue;

  // Stf request thread
  std::thread mStfRequestThread;
  std::shared_ptr<ConcurrentQueue<std::string> > mStfInputQueue;
  std::shared_ptr<ConcurrentQueue<ReceivedStfMeta> > mReceivedDataQueue;
  std::mutex mNumStfsRequestedLock;
    std::condition_variable mNumStfsRequestedCv;
  std::atomic<std::chrono::milliseconds> mRequestTimeoutMs = std::chrono::milliseconds(StfDataRequestGrpcTimeoutMsDefault);

  /// STF grpc request threads
  struct GrpcStfReqInfo {
    StfRequest mStfRequest;
    // Protected by mNumStfsRequestedLock
      std::size_t mNumStfSenders;
      std::size_t &mNumRequested;
      std::atomic_size_t &mNumExpectedStfs;
  };
  ConcurrentQueue<GrpcStfReqInfo> mGrpcStfRequestQueue;
  std::vector<std::thread> mStfRequestThreadPool;

  // Topological TFs
  std::mutex mTopoTfIdLock;
  std::uint64_t mTopoStfId = 1;
  std::map<std::string, std::unordered_map<std::uint64_t, std::uint64_t> > mTopoTfIdRenameMap;

  // Store how many STFs were requested per TfID. nullopt if requests are not finished
  std::mutex mStfsCountMapLock;
    std::map<TimeFrameIdType, std::uint64_t> mStfsCountMap;

  // monitor how long it takes to fetch stfs from each FLP
  std::mutex mStfDurationMapLock;
    std::map<
      std::uint64_t, /* TF ID */
      std::unordered_map<std::string, std::chrono::steady_clock::time_point> /* StfSender id -> time */
    > mStfReqDuration;

  /// TfBuilder Memory Resource
  SyncMemoryResources &mMemI;

  /// Discovery configuration
  std::shared_ptr<ConsulTfBuilder> mDiscoveryConfig;

  /// TfBuilder RPC server
  std::unique_ptr<Server> mServer;

  /// Scheduler RPC client
  StfSenderRpcClientCollection<ConsulTfBuilder> mStfSenderRpcClients;

  /// StfSender RPC clients
  TfSchedulerRpcClient mTfSchedulerRpcClient;

  /// TF buffer size accounting
  std::recursive_mutex mTfIdSizesLock;
    std::unordered_map <uint64_t, uint64_t> mTfIdSizes;
    // Update information for the TfScheduler
    std::uint64_t mBufferSize = 0;
    std::uint64_t mCurrentTfBufferSize = 0;
    std::uint64_t mLastBuiltTfId = 0;
    std::uint32_t mNumBufferedTfs = 0;
    std::int32_t mNumTfsInBuilding = 0;

  /// Queue of TF building requests
  std::unique_ptr<ConcurrentFifo<TfBuildingInformation>> mTfBuildRequests = nullptr;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_BUILDER_RPC_H_ */
