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

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

struct StfInfo {
  struct ProxyStfInfo {};

  StfSenderStfInfo mStfInfo;
  bool mProxy = false; // if completed by the future stf update

  StfInfo() = delete;
  explicit StfInfo(const StfSenderStfInfo &&pStfInfo)
  : mStfInfo(std::move(pStfInfo)),
    mProxy(false)
  { }

  explicit StfInfo(ProxyStfInfo)
  : mProxy(true)
  { }

  const std::string& process_id() const { return mStfInfo.info().process_id(); }
  std::uint64_t stf_id() const { return mStfInfo.stf_id(); }
  std::uint64_t stf_size() const { return mStfInfo.stf_size(); }
};

struct TopoStfInfo {
  StfSenderStfInfo mStfInfo;
  char mDataOrigin[4]; // detector id
  std::uint64_t mSubSpec;

  TopoStfInfo() = delete;
  TopoStfInfo(const StfSenderStfInfo &pStfInfo, const std::string_view &pDataOrigin, const std::uint64_t pSubSpec)
  : mStfInfo(pStfInfo),
    mSubSpec(pSubSpec)
  {
    std::copy_n(pDataOrigin.cbegin(), 3, mDataOrigin); mDataOrigin[3] = '\0';
  }

  const std::string& process_id() const { return mStfInfo.info().process_id(); }
  std::uint64_t stf_id() const { return mStfInfo.stf_id(); }
  std::uint64_t stf_size() const { return mStfInfo.stf_size(); }
};

class TfSchedulerStfInfo
{
public:
  TfSchedulerStfInfo() = delete;
  TfSchedulerStfInfo(std::shared_ptr<ConsulTfScheduler> pDiscoveryConfig,
                     TfSchedulerConnManager &pConnManager,
                     TfSchedulerTfBuilderInfo &pTfBuilderInfo)
  : mDiscoveryConfig(pDiscoveryConfig),
    mConnManager(pConnManager),
    mTfBuilderInfo(pTfBuilderInfo),
    mDroppedStfs(24ULL * 3600 * 88), // 1h of running ~ 1MiB size
    mBuiltTfs(24ULL * 3600 * 88)
  { }

  ~TfSchedulerStfInfo() { }

  void start() {
    mStfInfoMap.clear();

    mRunning = true;
    // Start the scheduling threads
    mCompletingThread = create_thread_member("tf_completer", &TfSchedulerStfInfo::TfCompleterThread, this);
    mSchedulingThread = create_thread_member("sched_sched", &TfSchedulerStfInfo::SchedulingThread, this);
    mStaleStfThread = create_thread_member("stale_drop", &TfSchedulerStfInfo::StaleCleanupThread, this);
    mWatermarkThread = create_thread_member("wmark", &TfSchedulerStfInfo::HighWatermarkThread, this);
    mDropThread = create_thread_member("sched_drop", &TfSchedulerStfInfo::DropThread, this);
    // Topological distribution
    mTopoSchedulingThread = create_thread_member("sched_topo_sched", &TfSchedulerStfInfo::TopoSchedulingThread, this);
  }

  void stop() {
    DDDLOG("TfSchedulerStfInfo::stop()");
    mRunning = false;
    mDropQueue.stop();
    mCompleteStfsInfoQueue.stop();
    mTopoStfInfoQueue.stop();

    mReportedStfInfoQueue.stop();
    if (mCompletingThread.joinable()) {
      mCompletingThread.join();
    }

    if (mSchedulingThread.joinable()) {
      DDDLOG("Waiting on TfSchedulerStfInfo::SchedulingThread");
      mSchedulingThread.join();
    }

    if (mStaleStfThread.joinable()) {
      DDDLOG("Waiting on TfSchedulerStfInfo::StaleCleanupThread");
      mStaleStfThread.join();
    }

    if (mWatermarkThread.joinable()) {
      DDDLOG("Waiting on TfSchedulerStfInfo::HighWatermarkThread");
      mWatermarkThread.join();
    }

    if (mDropThread.joinable()) {
      DDDLOG("Waiting on TfSchedulerStfInfo::DropThread");
      mDropThread.join();
    }

    if (mTopoSchedulingThread.joinable()) {
      DDDLOG("Waiting on TfSchedulerStfInfo::TopoSchedulingThread");
      mTopoSchedulingThread.join();
    }

    // delete all stf information
    std::unique_lock lLock(mGlobalStfInfoLock);
    mStfInfoMap.clear();

    DDDLOG("Finished TfSchedulerStfInfo::stop");
  }

  void addStfInfo(const StfSenderStfInfo &pStfInfo, SchedulerStfInfoResponse &pResponse);

  void TfCompleterThread();
  void SchedulingThread();
  void TopoSchedulingThread();
  void StaleCleanupThread();
  void HighWatermarkThread();
  void DropThread();


private:
  /// Discard timeout for incomplete TFs
  static constexpr auto sStfDiscardTimeout = 5s;

  std::atomic_bool mRunning = false;

  /// Discovery configuration
  std::shared_ptr<ConsulTfScheduler> mDiscoveryConfig;

  /// RPC clients to StfSenders and TfBuilders
  TfSchedulerConnManager &mConnManager;

  /// Collect information on TfBuilders
  TfSchedulerTfBuilderInfo &mTfBuilderInfo;

  /// Drop thread & queue
  ConcurrentFifo<std::tuple<std::uint64_t, std::string>> mDropQueue;
  std::thread mDropThread;

  /// GLOBAL RUN: Stfs global info
  mutable std::mutex mGlobalStfInfoLock;
    std::condition_variable mMemWatermarkCondition;
    std::condition_variable mStaleCondition;
    std::map<std::uint64_t, std::map< std::string, StfInfo>> mStfInfoMap;
    std::map<std::uint64_t, std::chrono::steady_clock::time_point> mStfInfoStartTimeMap;
    std::map<std::uint64_t, bool> mStfInfoIncomplete;

    std::map<std::string, StfSenderInfo> mStfSenderInfoMap;
    std::uint64_t mRunNumber = 0;

    std::atomic_uint64_t mNotScheduledTfsCount = 0;

    /// TfCompleterThread
    std::atomic_uint64_t mStaleStfTimeoutMs = StaleTfTimeoutMsDefault;
    std::uint64_t mCompleteTfCount = 0;
    std::atomic_uint64_t mIncompleteTfCount = 0;
    std::uint64_t mLastStfId = 0;
    std::uint64_t mMaxCompletedTfId = 0;
    std::map<std::string, std::uint64_t> mMaxStfIdPerStfSender;
    // total sizes
    std::uint64_t mTfSizeTotalScheduled = 0;
    std::uint64_t mTfSizeTotalRejected = 0;

    std::uint64_t mStaleTfCount = 0;
    std::uint64_t mScheduledTfs = 0;

    EventRecorder mDroppedStfs;
    EventRecorder mBuiltTfs;

    void reset() {
      // NOTE: only call when holding mGlobalStfInfoLock
      mLastStfId = 0;
      mMaxCompletedTfId = 0;
      mNotScheduledTfsCount = 0;

      mCompleteTfCount = 0;
      mIncompleteTfCount = 0;
      mMaxStfIdPerStfSender.clear();

      mStaleTfCount = 0;
      mScheduledTfs = 0;

      mTfSizeTotalScheduled = 0;
      mTfSizeTotalRejected = 0;

      mDroppedStfs.reset();
      mBuiltTfs.reset();

      if (!mStfInfoMap.empty()) {
        WDDLOG("TfSchedulerStfInfo::reset(): StfInfoMap not empty. size={}", mStfInfoMap.size());
      }
      mStfInfoMap.clear();
    }

    inline void requestDropSingle(const std::uint64_t lStfId, const std::string &pStfsId) {
      // NOTE: this is called on late, invalid, or duplicate STF updates
      //       Do not set any global flags. The TF might have been dropped or incomplete

      mDropQueue.push(std::make_tuple(lStfId, pStfsId));
    }

    inline void requestDropAllLocked(const std::uint64_t pStfId, const std::string &pStfsId) {

      mStfInfoMap.erase(pStfId);

      // first check if this was scheduled. There could be a race with incomplete scheduling
      if (mBuiltTfs.GetEvent(pStfId)) {
        // only drop this particular instance
        mDropQueue.push(std::make_tuple(pStfId, pStfsId));
      } else {

        if (mDroppedStfs.GetEvent(pStfId) == false) {
          mDroppedStfs.SetEvent(pStfId);
          mNotScheduledTfsCount++;
        }
        mDropQueue.push(std::make_tuple(pStfId, ""));
      }
    }

    inline void requestDropAllUnlocked(const std::uint64_t pStfId, const std::string &pStfsId) {
      std::scoped_lock lLock(mGlobalStfInfoLock);

      requestDropAllLocked(pStfId, pStfsId);
    }

  inline void requestDropAllFromSchedule(const std::uint64_t lStfId, const std::uint64_t pInc = 1) {
    std::scoped_lock lLock(mGlobalStfInfoLock);
    if (mDroppedStfs.GetEvent(lStfId) != false) {
      DDDLOG_RL(10000, "Request for dropping of already discarded TimeFrame. tf_id={}", lStfId);
    } else {
      mDroppedStfs.SetEvent(lStfId);
      mNotScheduledTfsCount += pInc;
    }
    mStfInfoMap.erase(lStfId);
    mDropQueue.push(std::make_tuple(lStfId, ""));
  }

  inline void requestDropTopoStf(const std::uint64_t lStfId, const std::string &pStfsId) {
    mDropQueue.push(std::make_tuple(lStfId, pStfsId));
    mNotScheduledTfsCount++;
  }

  /// tf completing thread
  ConcurrentQueue<StfInfo> mReportedStfInfoQueue;
  std::thread mCompletingThread;

  /// scheduling thread & queue
  // <complete flag ,StfInfoMao>
  ConcurrentQueue<std::tuple<bool, std::map<std::string, StfInfo>>> mCompleteStfsInfoQueue;
  std::thread mSchedulingThread;

  /// memory watermark thread
  std::thread mWatermarkThread;

  /// stale cleanup thread
  std::thread mStaleStfThread;

  /// TOPOLOGY RUN: Stfs global info
  void addTopologyStfInfo(const StfSenderStfInfo &pStfInfo, SchedulerStfInfoResponse &pResponse);

  /// scheduling thread & queue
  ConcurrentQueue<std::unique_ptr<TopoStfInfo>> mTopoStfInfoQueue;
  std::thread mTopoSchedulingThread;

};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_SCHEDULER_STF_INFO_H_ */
