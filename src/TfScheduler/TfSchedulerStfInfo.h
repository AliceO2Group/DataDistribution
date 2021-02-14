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

#ifndef ALICEO2_TF_SCHEDULER_STF_INFO_H_
#define ALICEO2_TF_SCHEDULER_STF_INFO_H_

#include "TfSchedulerTfBuilderInfo.h"
#include "TfSchedulerConnManager.h"

#include <ConfigParameters.h>
#include <ConfigConsul.h>

#include <StfSenderRpcClient.h>

#include <discovery.pb.h>
#include <discovery.grpc.pb.h>
#include <grpcpp/grpcpp.h>

#include <Utilities.h>

#include <vector>
#include <map>
#include <thread>
#include <chrono>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

struct StfInfo {
  std::chrono::system_clock::time_point mUpdateLocalTime;
  StfSenderStfInfo mStfInfo;
  bool mIsScheduled = false;

  StfInfo() = delete;
  StfInfo(std::chrono::system_clock::time_point pUpdateLocalTime, const StfSenderStfInfo &pStfInfo)
  : mUpdateLocalTime(pUpdateLocalTime),
    mStfInfo(pStfInfo)
  {
  }

  const std::string& process_id() const { return mStfInfo.info().process_id(); }
  std::uint64_t stf_id() const { return mStfInfo.stf_id(); }
  std::uint64_t stf_size() const { return mStfInfo.stf_size(); }
};

class TfSchedulerStfInfo
{
public:
  TfSchedulerStfInfo() = delete;
  TfSchedulerStfInfo(std::shared_ptr<ConsulTfSchedulerInstance> pDiscoveryConfig,
                     TfSchedulerConnManager &pConnManager,
                     TfSchedulerTfBuilderInfo &pTfBuilderInfo)
  : mDiscoveryConfig(pDiscoveryConfig),
    mConnManager(pConnManager),
    mTfBuilderInfo(pTfBuilderInfo)
  {

  }

  ~TfSchedulerStfInfo() { }

  void start() {
    mStfInfoMap.clear();

    mRunning = true;
    // Start the scheduling thread
    mSchedulingThread = create_thread_member("sched_sched", &TfSchedulerStfInfo::SchedulingThread, this);
  }

  void stop() {
    mRunning = false;

    {
      std::unique_lock lLock(mCompleteStfInfoLock);
      mCompleteStfsInfo.clear();
      mStfScheduleCondition.notify_all();
    }

    if (mSchedulingThread.joinable()) {
      mSchedulingThread.join();
    }

    // delete all stf information
    mStfInfoMap.clear();
  }

  void SchedulingThread();
  void addStfInfo(const StfSenderStfInfo &pStfInfo, SchedulerStfInfoResponse &pResponse);

private:
  /// Discard timeout for incomplete TFs
  static constexpr auto sStfDiscardTimeout = 10s;

  /// Discovery configuration
  std::shared_ptr<ConsulTfSchedulerInstance> mDiscoveryConfig;

  /// RPC clients to StfSenders and TfBuilders
  TfSchedulerConnManager &mConnManager;

  /// Collect information on TfBuilders
  TfSchedulerTfBuilderInfo &mTfBuilderInfo;

  /// Housekeeping thread
  std::atomic_bool mRunning = false;
  std::thread mSchedulingThread;

  /// Stfs global info
  mutable std::mutex mGlobalStfInfoLock;
  std::map<std::uint64_t, std::vector<StfInfo>> mStfInfoMap;
  std::uint64_t mLastStfId = 0;

  /// Stfs for scheduling
  mutable std::mutex mCompleteStfInfoLock;
  std::condition_variable mStfScheduleCondition;
  std::deque<std::vector<StfInfo>> mCompleteStfsInfo;

};
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_SCHEDULER_STF_INFO_H_ */
