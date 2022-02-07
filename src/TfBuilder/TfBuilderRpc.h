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
  bool start(const std::uint64_t pBufferSize, std::shared_ptr<ConcurrentQueue<ReceivedStfMeta> > pRecvQueue);
  void stop();

  void startAcceptingTfs();
  void stopAcceptingTfs();

  void UpdateSendingThread();
  void StfRequestThread();

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
    mTfBuildRequests->flush();
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

  // Stf request thread
  std::thread mStfRequestThread;
  std::shared_ptr<ConcurrentQueue<ReceivedStfMeta> > mReceivedDataQueue;

    struct StfRequests {
      std::string mStfSenderId;
      std::uint64_t mStfDataSize;
      StfDataRequestMessage mRequest;

      StfRequests() = default;
      StfRequests(const std::string &pStfSenderId, const std::uint64_t pStfDataSize, const StfDataRequestMessage &pRequest)
      : mStfSenderId(pStfSenderId), mStfDataSize(pStfDataSize), mRequest(pRequest) { }
    };

  std::atomic_int64_t mMaxNumReqInFlight = 64;
  std::atomic_int64_t mNumReqInFlight = 0;

  // <tfid, topo?, topo_id, stf_requests>
  ConcurrentFifo<std::tuple<std::uint64_t, bool, std::uint64_t, std::vector<StfRequests>> >  mStfRequestQueue;

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
  std::unique_ptr<ConcurrentFifo<TfBuildingInformation>> mTfBuildRequests;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_BUILDER_RPC_H_ */
