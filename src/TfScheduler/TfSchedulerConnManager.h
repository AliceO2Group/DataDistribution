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

#ifndef ALICEO2_TF_SCHEDULER_CONNMANAGER_H_
#define ALICEO2_TF_SCHEDULER_CONNMANAGER_H_

#include <ConfigParameters.h>
#include <ConfigConsul.h>

#include <StfSenderRpcClient.h>
#include <TfBuilderRpcClient.h>

#include <discovery.pb.h>
#include <discovery.grpc.pb.h>
#include <grpcpp/grpcpp.h>

#include <Utilities.h>

#include <vector>
#include <map>
#include <set>
#include <thread>
#include <list>
#include <shared_mutex>

namespace o2::DataDistribution
{

enum StfSenderState {
  STF_SENDER_STATE_OK = 1,
  STF_SENDER_STATE_INITIALIZING,
  STF_SENDER_STATE_INCOMPLETE
};


struct StfSenderUCXConnectReq {
  TfBuilderUCXEndpoint mRpcReq;
  ConcurrentQueue<std::tuple<bool, std::string, ConnectTfBuilderUCXResponse>> *mConnRepQueue;

  StfSenderUCXConnectReq(const TfBuilderUCXEndpoint &pReq, ConcurrentQueue<std::tuple<bool, std::string, ConnectTfBuilderUCXResponse>> *pQueue)
  : mConnRepQueue(pQueue)
  {
    mRpcReq.CopyFrom(pReq);
  }
};

struct StfSenderUCXThreadInfo {
  std::unique_ptr<ConcurrentQueue<std::unique_ptr<StfSenderUCXConnectReq>>> mConnReqQueue;

  StfSenderUCXThreadInfo() {
    mConnReqQueue = std::make_unique<ConcurrentQueue<std::unique_ptr<StfSenderUCXConnectReq>>>();
  }
};

class TfSchedulerConnManager
{
 public:
  TfSchedulerConnManager() = delete;
  TfSchedulerConnManager(std::shared_ptr<ConsulTfScheduler> pDiscoveryConfig, const PartitionRequest &pPartitionRequest)
  : mPartitionInfo(pPartitionRequest),
    mDiscoveryConfig(pDiscoveryConfig),
    mStfSenderRpcClients(pDiscoveryConfig),
    mTfBuilderRpcClients(pDiscoveryConfig)
  {
  }

  ~TfSchedulerConnManager() { }

  bool start();
  void stop();
  std::size_t checkStfSenders();

  bool stfSendersReady() { return mStfSenderRpcClients.started() && (mStfSenderRpcClients.size() == mPartitionInfo.mStfSenderIdList.size()); }

  std::set<std::string> getStfSenderSet() const
  {
    std::set<std::string> lSet;
    const auto &lIdList = mDiscoveryConfig->status().stf_sender_id_list();
    lSet.insert(lIdList.begin(), lIdList.end());
    return lSet;
  }

  void StfSenderMonitoringThread();
  void DropStfThread();

  /// Partition RPCs
  bool requestTfBuildersTerminate();
  bool requestStfSendersTerminate();

  /// External requests by TfBuilders
  void connectTfBuilder(const TfBuilderConfigStatus &pTfBuilderStatus, TfBuilderConnectionResponse &pResponse /*out*/);
  void disconnectTfBuilder(const TfBuilderConfigStatus &pTfBuilderStatus, StatusResponse &pResponse /*out*/);

  /// External requests by TfBuilders UCX frontend
  void connectTfBuilderUCX(const TfBuilderConfigStatus &pTfBuilderStatus, TfBuilderUCXConnectionResponse &pResponse /*out*/);
  void disconnectTfBuilderUCX(const TfBuilderConfigStatus &pTfBuilderStatus, StatusResponse &pResponse /*out*/);
  void ConnectTfBuilderUCXThread(const std::string lStfSenderId);

  /// Internal request, disconnect on error
  void removeTfBuilder(const std::string &pTfBuilderId);

  /// Drop all SubTimeFrames (in case they can't be scheduled)
  void dropAllStfsAsync(const std::uint64_t pStfId);
  /// Drop a single SubTimeFrames (in case they can't be scheduled)
  void dropSingleStfsAsync(const std::uint64_t pStfId, const std::string &pStfSenderId);

  bool newTfBuilderRpcClient(const std::string &pId) { return mTfBuilderRpcClients.add_if_new(pId); }
  void deleteTfBuilderRpcClient(const std::string &pId) { mTfBuilderRpcClients.remove(pId); }

  TfBuilderRpcClient getTfBuilderRpcClient(const std::string &pId)
  {
    return mTfBuilderRpcClients.get(pId);
  }

  bool checkStfSenderRpcConn(const std::string &lStfSenderId)
  {
    return mStfSenderRpcClients.checkStfSenderRpcConn(lStfSenderId);
  }


  StfSenderState getStfSenderState() const { return mStfSenderState; }
  std::size_t getStfSenderCount() {
    std::scoped_lock lLock(mStfSenderClientsLock);
    return mStfSenderRpcClients.size();
  }

  void setMonitorDuration(const bool pMon) {
    mStfSenderRpcClients.setMonitorDuration(pMon);
    mTfBuilderRpcClients.setMonitorDuration(pMon);
  }

private:
  /// Partition information
  PartitionRequest mPartitionInfo;

  /// StfSender state
  std::atomic<StfSenderState> mStfSenderState = STF_SENDER_STATE_INITIALIZING;

  /// Discovery configuration
  std::shared_ptr<ConsulTfScheduler> mDiscoveryConfig;

  /// Connection threads
  std::shared_mutex mConnectInfoLock;
    std::map<std::string, StfSenderUCXThreadInfo> mConnectThreadInfos;
    std::vector<std::thread> mConnectionThreads;

  /// Scheduler threads
  std::atomic_bool mRunning = false;
  std::thread mStfSenderMonitoringThread;

  /// StfSender RPC-client channels
  std::shared_mutex mStfSenderClientsLock;
    StfSenderRpcClientCollection<ConsulTfScheduler> mStfSenderRpcClients;

  /// TfBuilder RPC-client channels
  TfBuilderRpcClientCollection<ConsulTfScheduler> mTfBuilderRpcClients;

  /// Stf drop
  ConcurrentQueue<std::pair<std::string, std::uint64_t>> mStfDropQueue;
  std::vector<std::thread> mStfDropThreads;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_SCHEDULER_CONNMANAGER_H_ */
