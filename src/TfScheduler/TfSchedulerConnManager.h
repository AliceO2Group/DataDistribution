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

namespace o2
{
namespace DataDistribution
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
  TfSchedulerConnManager(std::shared_ptr<ConsulTfSchedulerInstance> pDiscoveryConfig, const PartitionRequest &pPartitionRequest)
  : mPartitionInfo(pPartitionRequest),
    mDiscoveryConfig(pDiscoveryConfig),
    mStfSenderRpcClients(pDiscoveryConfig),
    mTfBuilderRpcClients(pDiscoveryConfig)
  {
  }

  ~TfSchedulerConnManager() { }

  bool start() {
    using namespace std::chrono_literals;

    while (!mStfSenderRpcClients.start()) {
      std::this_thread::sleep_for(1s);
    }

    mRunning = true;

    // start gRPC client monitoring thread
    mStfSenderMonitoringThread = create_thread_member("sched_stfs_mon",
      &TfSchedulerConnManager::StfSenderMonitoringThread, this);
    return true;
  }

  void stop() {
    mRunning = false;

    if (mStfSenderMonitoringThread.joinable()) {
      mStfSenderMonitoringThread.join();
    }

    // delete all rpc clients
    mStfSenderRpcClients.stop();
  }

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

  /// External requests by TfBuilders
  void connectTfBuilder(const TfBuilderConfigStatus &pTfBuilderStatus, TfBuilderConnectionResponse &pResponse /*out*/);
  void disconnectTfBuilder(const TfBuilderConfigStatus &pTfBuilderStatus, StatusResponse &pResponse /*out*/);
  /// Internal request, disconnect on error
  void removeTfBuilder(const std::string &pTfBuilderId);

  /// Drop all SubTimeFrames (in case they can't be scheduled)
  void dropAllStfsAsync(const std::uint64_t pStfId);

  bool newTfBuilderRpcClient(const std::string &pId)
  {
    return mTfBuilderRpcClients.add(pId);
  }

  void deleteTfBuilderRpcClient(const std::string &pId)
  {
    mTfBuilderRpcClients.remove(pId);
  }

  TfBuilderRpcClient getTfBuilderRpcClient(const std::string &pId)
  {
    return mTfBuilderRpcClients.get(pId);
  }

  bool checkStfSenderRpcConn(const std::string &lStfSenderId)
  {
    return mStfSenderRpcClients.checkStfSenderRpcConn(lStfSenderId);
  }

  StfSenderState getStfSenderState() const { return mStfSenderState; }

private:
  /// Partition information
  PartitionRequest mPartitionInfo;

  /// StfSender state
  std::atomic<StfSenderState> mStfSenderState = STF_SENDER_STATE_INITIALIZING;

  /// Discovery configuration
  std::shared_ptr<ConsulTfSchedulerInstance> mDiscoveryConfig;

  /// Scheduler threads
  bool mRunning = false;
  std::thread mStfSenderMonitoringThread;

  /// StfSender RPC-client channels
  std::recursive_mutex mStfSenderClientsLock;
  StfSenderRpcClientCollection<ConsulTfSchedulerInstance> mStfSenderRpcClients;
  /// TfBuilder RPC-client channels
  TfBuilderRpcClientCollection<ConsulTfSchedulerInstance> mTfBuilderRpcClients;

  /// futures for async operation
  std::recursive_mutex mStfDropFuturesLock;
  std::list<std::future<std::uint64_t>> mStfDropFutures;
};
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_SCHEDULER_CONNMANAGER_H_ */
