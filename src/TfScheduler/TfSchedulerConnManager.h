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
#include <future>

namespace o2::DataDistribution
{

enum StfSenderState {
  STF_SENDER_STATE_OK = 1,
  STF_SENDER_STATE_INITIALIZING,
  STF_SENDER_STATE_INCOMPLETE
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

  bool stfSendersReady() { return mStfSenderRpcClients.size() == mPartitionInfo.mStfSenderIdList.size(); }

  std::set<std::string> getStfSenderSet() const
  {
    std::set<std::string> lSet;
    const auto &lIdList = mDiscoveryConfig->status().stf_sender_id_list();
    lSet.insert(lIdList.begin(), lIdList.end());
    return lSet;
  }

  void StfSenderMonitoringThread();
  void DropWaitThread();

  /// Partition RPCs
  bool requestTfBuildersTerminate();
  bool requestStfSendersTerminate();

  /// External requests by TfBuilders
  void connectTfBuilder(const TfBuilderConfigStatus &pTfBuilderStatus, TfBuilderConnectionResponse &pResponse /*out*/);
  void disconnectTfBuilder(const TfBuilderConfigStatus &pTfBuilderStatus, StatusResponse &pResponse /*out*/);

  /// External requests by TfBuilders UCX frontend
  void connectTfBuilderUCX(const TfBuilderConfigStatus &pTfBuilderStatus, TfBuilderUCXConnectionResponse &pResponse /*out*/);
  void disconnectTfBuilderUCX(const TfBuilderConfigStatus &pTfBuilderStatus, StatusResponse &pResponse /*out*/);

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

  void deleteStfSenderRpcClient(const std::string &pId)
  {
    mStfSenderRpcClients.remove(pId);
  }

  StfSenderState getStfSenderState() const { return mStfSenderState; }
  std::size_t getStfSenderCount() {
    std::scoped_lock lLock(mStfSenderClientsLock);
    return mStfSenderRpcClients.size();
  }

private:
  /// Partition information
  PartitionRequest mPartitionInfo;

  /// StfSender state
  std::atomic<StfSenderState> mStfSenderState = STF_SENDER_STATE_INITIALIZING;

  /// Discovery configuration
  std::shared_ptr<ConsulTfScheduler> mDiscoveryConfig;

  /// Scheduler threads
  bool mRunning = false;
  std::thread mStfSenderMonitoringThread;
  std::thread mDropFutureWaitThread;

  /// StfSender RPC-client channels
  std::recursive_mutex mStfSenderClientsLock;
    StfSenderRpcClientCollection<ConsulTfScheduler> mStfSenderRpcClients;
  /// TfBuilder RPC-client channels
  TfBuilderRpcClientCollection<ConsulTfScheduler> mTfBuilderRpcClients;

  /// futures for async operation
  std::mutex mStfDropFuturesLock;
    std::condition_variable_any mStfDropFuturesCV;
    std::list<std::future<std::uint64_t>> mStfDropFutures;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_SCHEDULER_CONNMANAGER_H_ */
