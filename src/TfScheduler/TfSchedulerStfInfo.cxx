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

#include "TfSchedulerStfInfo.h"
#include "TfSchedulerInstanceRpc.h"

#include <StfSenderRpcClient.h>

#include <DataDistMonitoring.h>

#include <boost/algorithm/string/join.hpp>

#include <set>
#include <tuple>
#include <algorithm>
#include <random>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

void TfSchedulerStfInfo::SchedulingThread()
{
  DataDistLogger::SetThreadName("SchedulingThread");
  DDDLOG("Starting StfInfo Scheduling thread.");

  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();

  std::uint64_t lNumTfScheds = 0;

  // count how many time an FLP was missing STFs
  std::map<std::string, std::uint64_t> lStfSenderMissingCnt;
  std::optional<std::tuple<bool, std::map<std::string, StfInfo>>> lStfInfosOpt;

  // Build or discard
  bool lBuildIncomplete = mDiscoveryConfig->getBoolParam(BuildIncompleteTfsKey, BuildIncompleteTfsValue);
  IDDLOG("TfScheduler: Building of incomplete TimeFrames is {}.", lBuildIncomplete ? "enabled" : "disabled");

  DDMON_RATE("tfscheduler", "tf.scheduled.tf", 0.0);
  DDMON_RATE("tfscheduler", "tf.rejected.tf", 0.0);

  while ((lStfInfosOpt = mCompleteStfsInfoQueue.pop()) != std::nullopt) {

    DDMON("tfscheduler", "tf.rejected.total", mNotScheduledTfsCount);
    DDMON("tfscheduler", "tf.scheduled.total", mScheduledTfs);

    std::tuple<bool, std::map<std::string, StfInfo>> &lStfInfosTuple = lStfInfosOpt.value();
    const bool lTfComplete = std::get<0>(lStfInfosTuple);
    std::map<std::string, StfInfo> &lStfInfos = std::get<1>(lStfInfosTuple);

    TfBuildingInformation lRequest;

    // check complete stf information
    assert (!lStfInfos.empty());

    // remove proxy stfs
    if (!lTfComplete) {
      // std::erase_if(lStfInfos, [](const auto &s) { return (s.mProxy == true);} );
      for (auto i = lStfInfos.begin(), last = lStfInfos.end(); i != last; ) {
        if (i->second.mProxy) {
          i = lStfInfos.erase(i);
        } else {
          ++i;
        }
      }
    }

    const auto lTfId = lStfInfos.begin()->second.stf_id();

    if (lStfInfos.size() != lNumStfSenders) {
      assert (!lTfComplete);
      assert(lStfInfos.size() < lNumStfSenders);

      if (lBuildIncomplete) {
        DDMON("tfscheduler", "tf.scheduled.incomplete.stf_cnt", lStfInfos.size());
      } else {
        WDDLOG_RL(5000, "StaleStfDropThread: TFs have been discarded due to incomplete number of STFs. total={}", mStaleTfCount);
        requestDropAllFromSchedule(lTfId);
        continue;
      }
    }

    bool lScheduleSuccess = false;

    // calculate combined STF size
    // we could have empty resulting TF.
    // E.g. FLP-DPL can remove all data, or filtered out empty frames in trigger more
    std::uint64_t lTfSize = 0;
    for (const auto &lStfI : lStfInfos) {
      lTfSize += lStfI.second.stf_size();
      (*lRequest.mutable_stf_size_map())[lStfI.second.process_id()] = lStfI.second.stf_size();
    }

    // check for TF size. Discard zero-length TFs that have no detector data
    if (lTfSize == 0) {
      lScheduleSuccess = true; // count this as success for scheduler rates
      requestDropAllFromSchedule(lTfId, 0); // do not decrement rejected tf counter
    } else {

      lRequest.set_tf_id(lTfId);
      lRequest.set_tf_size(lTfSize);
      lRequest.set_tf_source(StfSource::DEFAULT);

      // Get the best TfBuilder candidate
      std::string lTfBuilderId;
      if (mTfBuilderInfo.findTfBuilderForTf(lTfSize, lTfBuilderId /*out*/) ) {
        lNumTfScheds++;
        DDDLOG_RL(10000, "Scheduling new TF. tf_id={} tfb_id={} total={}", lTfId, lTfBuilderId, lNumTfScheds);

        assert (!lTfBuilderId.empty());
        // Notify TfBuilder to build the TF
        TfBuilderRpcClient lRpcCli = mConnManager.getTfBuilderRpcClient(lTfBuilderId);

        // finding and getting the client is racy
        if (lRpcCli) {
          BuildTfResponse lResponse;

          if (lRpcCli.get().BuildTfRequest(lRequest, lResponse)) {
            switch (lResponse.status()) {
              case BuildTfResponse::OK:
                // marked TfBuilder as scheduled
                lScheduleSuccess = true;
                mTfBuilderInfo.markTfBuilderWithTfId(lTfBuilderId, lRequest.tf_id());
                break;
              case BuildTfResponse::ERROR_NOMEM:
                EDDLOG_RL(1000, "Scheduling error: selected TfBuilder returned ERROR_NOMEM. tf_id={:s}", lTfBuilderId);
                requestDropAllFromSchedule(lTfId);
                break;
              case BuildTfResponse::ERROR_NOT_RUNNING:
                EDDLOG_RL(1000, "Scheduling error: selected TfBuilder returned ERROR_NOT_RUNNING. tf_id={:s}", lTfBuilderId);
                requestDropAllFromSchedule(lTfId);
                break;
              default:
                break;
            }
          } else {
            WDDLOG_RL(10000, "Scheduling of Tf failed. to_tfb_id={} reason=grpc_error", lTfBuilderId);
            WDDLOG("Removing TfBuilder from scheduling. tfb_id={} reason=BuildTfRequest_failed", lTfBuilderId);

            lRpcCli.put();

            // NOTE: if tfbuilder was not reached, stfsender will be able to clean the stfs on their own
            //       if tfbuilder was reached (grpc timeout), it will continue to fetch the TF
            // requestDropAllFromSchedule(lTfId);

            mConnManager.removeTfBuilder(lTfBuilderId);
            mTfBuilderInfo.removeReadyTfBuilder(lTfBuilderId);
          }
        } else {
          // TfBuilder was removed in the meantime, e.g. by housekeeping thread because of stale info
          // We drop the current TF as this is not a likely situation
          WDDLOG("Selected TfBuilder is not currently reachable. TF will be dropped. tfb_id={} tf_id={}",
            lTfBuilderId, lTfId);

          requestDropAllFromSchedule(lTfId);
          mTfBuilderInfo.removeReadyTfBuilder(lTfBuilderId);
          // mConnManager.removeTfBuilder(lTfBuilderId);
        }
      } else {
        // No candidate for scheduling
        requestDropAllFromSchedule(lTfId);
      }
    }

    if (lScheduleSuccess) {
      mScheduledTfs += 1;
      mTfSizeTotalScheduled += lTfSize;
      DDMON_RATE("tfscheduler", "tf.scheduled.tf", lTfSize);
      DDMON("tfscheduler", "tf.scheduled.data_size_total", mTfSizeTotalScheduled);
    } else {
      mTfSizeTotalRejected += lTfSize;
      DDMON_RATE("tfscheduler", "tf.rejected.tf", lTfSize);
      DDMON("tfscheduler", "tf.rejected.data_size_total", mTfSizeTotalRejected);
    }
  }

  DDDLOG("Exiting StfInfo Scheduling thread.");
}

// Mostly usefull for troubleshooting now when the high watermark thread is implemented
void TfSchedulerStfInfo::StaleCleanupThread()
{
  DataDistLogger::SetThreadName("StaleCleanupThread");
  DDDLOG("Starting StfInfo StaleCleanupThread thread.");

  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const std::set<std::string> lStfSenderIdSet = mConnManager.getStfSenderSet();
  std::vector<std::uint64_t> lStaleStfsToComplete;

  // count how many time an FLP was missing STFs
  std::map<std::string, std::uint64_t> lStfSenderMissingCnt;

  std::vector<StfInfo> lStfInfos;

  // update housekeeping parameters
  mStaleStfTimeoutMs = std::clamp(mDiscoveryConfig->getUInt64Param(StaleTfTimeoutMsKey, StaleTfTimeoutMsDefault),
      std::uint64_t(250), std::uint64_t(60000));

  IDDLOG("Parameter updated (consul) {}={}", StaleTfTimeoutMsKey, mStaleStfTimeoutMs);

  while (mRunning) {

    lStaleStfsToComplete.clear();
    // update housekeeping parameters
    const auto lStaleStfTimeoutMs = std::clamp(mDiscoveryConfig->getUInt64Param(StaleTfTimeoutMsKey, StaleTfTimeoutMsDefault),
      std::uint64_t(25), std::uint64_t(60000));
    if (lStaleStfTimeoutMs != mStaleStfTimeoutMs) {
      mStaleStfTimeoutMs = lStaleStfTimeoutMs;
      IDDLOG("Parameter updated (consul) {}={}", StaleTfTimeoutMsKey, mStaleStfTimeoutMs);
    }

    {
      std::unique_lock lLock(mGlobalStfInfoLock);
      mStaleCondition.wait_for(lLock, 100ms);

      // check available stf information
      for (const auto &lStfIdInfo : mStfInfoMap) {
        auto lStfId = lStfIdInfo.first;
        auto &lStfInfoVec = lStfIdInfo.second;

        if (lStfInfoVec.empty()) { // this should not happen
          EDDLOG_RL(1000, "Discarding TimeFrame with no STF updates. stf_id={} received={} expected={}",
            lStfId, lStfInfoVec.size(), lNumStfSenders);

          lStaleStfsToComplete.push_back(lStfId);
          continue;
        }

        // check and schedule
        assert (mStfInfoStartTimeMap.count(lStfId) > 0);
        const double lTimeDiffMs = std::abs(since<std::chrono::milliseconds>(mStfInfoStartTimeMap[lStfId]));

        if (lTimeDiffMs >= mStaleStfTimeoutMs) {
          WDDLOG_RL(10000, "Scheduling incomplete TimeFrame on timeout. stf_id={} received_stfs={} expected_stfs={} timeout_ms={:.4}",
            lStfId, lStfInfoVec.size(), lNumStfSenders, lTimeDiffMs);

          DDMON("tfscheduler", "stf.update.stale_dur_ms", lTimeDiffMs);

          // erase the stale stf
          lStaleStfsToComplete.push_back(lStfId);

          // find missing StfSenders
          std::set<std::string> lMissingStfSenders = lStfSenderIdSet;
          for (const auto &lUpdate : lStfInfoVec) {
            lMissingStfSenders.erase(lUpdate.first);
          }

          for (const auto &lStf : lMissingStfSenders) {
            lStfSenderMissingCnt[lStf]++;
          }

          WDDLOG_RL(10000, "Missing SubTimeFrames from StfSenders: {}", boost::algorithm::join(lMissingStfSenders, ", "));
        } else {
          break; // stop checking as soon the early STFs are within the stale timeout
        }
      }

      // drop outside of the main iteration loop
      for(const auto &lStfIdToDrop : lStaleStfsToComplete) {
        // move info to the completed queue
        auto lStaleInfoNode = mStfInfoMap.extract(lStfIdToDrop);

        assert (lStfIdToDrop > mMaxCompletedTfId);
        mMaxCompletedTfId = std::max(mMaxCompletedTfId, lStfIdToDrop);

        // mark this as built (handled)
        mBuiltTfs.SetEvent(lStfIdToDrop);
        mCompleteStfsInfoQueue.push(std::make_tuple(false, std::move(lStaleInfoNode.mapped())));
      }
    }

    if (lStaleStfsToComplete.size() > 0) {

      mStaleTfCount += lStaleStfsToComplete.size();
      DDMON("tfscheduler", "tf.stale.total", mStaleTfCount);

      mIncompleteTfCount += lStaleStfsToComplete.size();
      DDMON("tfscheduler", "tf.incomplete.total", mIncompleteTfCount);

      for (const auto &lStfSenderCnt : lStfSenderMissingCnt) {
        if (lStfSenderCnt.second > 0) {
          DDDLOG_RL(10000, "StfSender with missing ids: stfsender_id={} missing_cnt={}",
            lStfSenderCnt.first, lStfSenderCnt.second);
        }
      }
    }
  }

  DDDLOG("Exiting StfInfo StaleCleanupThread thread.");
}

void TfSchedulerStfInfo::HighWatermarkThread()
{
  DataDistLogger::SetThreadName("HighWatermarkThread");
  DDDLOG("Starting HighWatermarkThread thread.");

  // const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const std::set<std::string> lStfSenderIdSet = mConnManager.getStfSenderSet();
  std::vector<std::uint64_t> lStfsToErase;
  lStfsToErase.reserve(1000);


  std::size_t lNoStfSenderBufferAvailableCnt = 0;

  while (mRunning) {
    std::vector<std::uint64_t> lStfsToDrop;
    std::uint64_t lStfsToDropSize = 0;
    double lBuffUtilMean = 0.0;
    double lBuffUtilMax = 0.0;
    std::string lStfSenderToFree;

    {
      std::unique_lock lLock(mGlobalStfInfoLock);
      if (std::cv_status::timeout == mMemWatermarkCondition.wait_for(lLock, 1s)) {
        DDDLOG_RL(10000, "HighWatermarkThread: Running the StfSender buffer utilization perodic scan.");
      } else {
        WDDLOG_RL(10000, "HighWatermarkThread: Running the scan.");
      }

      // check for problematic stf senders
      for (const auto &lUtil : mStfSenderInfoMap) {
        const auto &lStfs = lUtil.first;
        const auto &lStfsUtil = lUtil.second;

        const double lBufUtil = double(lStfsUtil.buffer_used()) / double(lStfsUtil.buffer_size());
        lBuffUtilMean += lBufUtil / mStfSenderInfoMap.size();

        // max util sender
        if (lBufUtil > lBuffUtilMax) {
          lBuffUtilMax = lBufUtil;
          lStfSenderToFree = lStfs;
        }

        // log problematic ones
        if (lBufUtil > 0.90) {
          WDDLOG_RL(1000, "HighWatermark: buffer utilization too high. stfs_id={} buffer_util={:.4} buffer_size={}",
            lStfs, lBufUtil, lStfsUtil.buffer_size());

          mStaleCondition.notify_one();
        }
      }

      DDDLOG_RL(5000, "HighWatermark: StfSender buffer utilization max_util={:.4} stfs_id={} mean_util={:.4}",
        lBuffUtilMax, lStfSenderToFree, lBuffUtilMean);

      DDMON("tfscheduler", "stfsender.buffer.max_util", lBuffUtilMax);

      if (lStfSenderToFree.empty() || lBuffUtilMax <= 0.90) {
        // make sure the info is regenerated fresh after clearing
        mStfSenderInfoMap.clear();
        continue;
      }

      // check if we have incomplete TFs smaller than the currently build one
      // since all StfSenders are required to send updates in order, all incomplete TFs before
      // the current can be discarded
      assert (!lStfSenderToFree.empty());

      auto lTimeNow = std::chrono::steady_clock::now();

      for (const auto &it : mStfInfoMap) {
        const auto &lStfId = it.first;
        const auto &lStfMap = it.second;

        // limit how much we drop
        if (lStfsToDropSize > (std::uint64_t(4) << 30)) {
          break;
        }

        assert (!lStfMap.empty());

        // check the TF ID
        if (lStfId > mMaxCompletedTfId) {
          break;
        }

        // check if the TF is too recent
        assert (mStfInfoStartTimeMap.count(lStfId) > 0);
        if (!lStfMap.empty() && (lTimeNow - mStfInfoStartTimeMap[lStfId] < 1s)) {
          break;
        }

        // check if this will help the problematic stf sender
        const auto &lFoundStfSender = lStfMap.find(lStfSenderToFree);
        if (lFoundStfSender == lStfMap.end()) {
          continue;
        }

        WDDLOG_GRL(1000, "STFUpdateComplete: dropping incomplete TF with lower id than the currently completed one. "
          "drop_tf_id={} complete_tf_id={}", lStfId, mMaxCompletedTfId);

        lStfsToDropSize += lFoundStfSender->second.stf_size();
        lStfsToDrop.push_back(lStfId);
      }
      // make sure the info is regenerated fresh after clearing
      mStfSenderInfoMap.clear();

      // note: must drop while the map is locked
      for (const auto lStfId :lStfsToDrop) {
        requestDropAllLocked(lStfId, ""); // this will remove the element from mStfInfoMap
      }
    }// unlock mGlobalStfInfoLock

    if (lStfsToDropSize > 0) {
      WDDLOG("HighWatermark: cleared dropped STFs from StfSender. stfs_id={} num_stfs_dropped={} dropped_size={}",
        lStfSenderToFree, lStfsToDrop.size(), lStfsToDropSize);
    }

    lNoStfSenderBufferAvailableCnt += lStfsToDrop.size();
    DDMON("tfscheduler", "tf.rejected.stfs_drop_size", lStfsToDropSize);
    DDMON("tfscheduler", "tf.rejected.total", mNotScheduledTfsCount);
  }

  DDDLOG("Exiting HighWatermarkThread thread.");
}


void TfSchedulerStfInfo::DropThread()
{
  while (mRunning) {
    // stfID, stfs_id (empty to drop all)
    std::tuple<std::uint64_t, std::string> lStfToDrop;

    while (mDropQueue.pop(lStfToDrop)) {

      const std::uint64_t lStfId = std::get<0>(lStfToDrop);
      const std::string &lStfsId = std::get<1>(lStfToDrop);

#if !defined(NDEBUG)
      { // Should remove earlier?
        std::unique_lock lLock(mGlobalStfInfoLock);
        assert (mStfInfoMap.count(lStfId) == 0);
      }
#endif

      if (lStfsId.empty()) {
        mConnManager.dropAllStfsAsync(lStfId); // drop all
      } else {
        mConnManager.dropSingleStfsAsync(lStfId, lStfsId); // drop one StfSender  (topological distribution)
      }
    }
  }

  DDDLOG("Exiting StfInfo Scheduler stf drop thread.");
}

void TfSchedulerStfInfo::addStfInfo(const StfSenderStfInfo &pStfInfo, SchedulerStfInfoResponse &pResponse)
{
  using namespace std::chrono_literals;
  static thread_local std::default_random_engine lGen;
  static thread_local std::uniform_real_distribution<double> lUniformDist(0, 100.0);

  const std::uint64_t lRunNumber = pStfInfo.partition().run_number();
  const auto lStfId = pStfInfo.stf_id();

  if (pStfInfo.stf_source() == StfSource::TOPOLOGICAL) {
    assert (pStfInfo.stf_source_info_size() == 1);
    return addTopologyStfInfo(pStfInfo, pResponse);
  }

  {
    DDDLOG_GRL(5000, "addStfInfo: stf info received. stf_id={}", lStfId);

    const auto &lStfSenderId = pStfInfo.info().process_id();
    {// serialize for global info updates
      std::unique_lock lLock(mGlobalStfInfoLock);

      // always record latest stfsender status for high watermark thread
      mStfSenderInfoMap[lStfSenderId] = pStfInfo.stfs_info();

      // check if we need to restart tf counters for a new run
      if (mRunNumber < lRunNumber) {
        IDDLOG("New RunNumber received. run_number={}", lRunNumber);
        // reset internal counters
        reset();
        mTfBuilderInfo.resetCounters();
        mRunNumber = lRunNumber;
        IDDLOG("NEW RUN NUMBER: {} {}", pStfInfo.partition().partition_id(), lRunNumber);
        DataDistMonitor::enable_datadist(mRunNumber, pStfInfo.partition().partition_id());

        // set log run number
        DataDistLogger::sRunNumber = mRunNumber;
        DataDistLogger::sRunNumberStr = std::to_string(mRunNumber);
        impl::DataDistLoggerCtx::InitInfoLogger();
      } else if (mRunNumber > lRunNumber) {
        EDDLOG_GRL(500, "New RunNumber is smaller than the previous. run_number={} prev_run_number={}", lRunNumber, mRunNumber);
        pResponse.set_status((!mRunning) ? SchedulerStfInfoResponse::DROP_NOT_RUNNING :
          SchedulerStfInfoResponse::DROP_SCHED_DISCARDED);
        return;
      }

      { // check if this tf is rejected because of throttling
        static std::uint64_t lThrottlingRejectedSize = 0;

        if (lStfId > mLastThrottledStfId) {

          // decide if discarding on throttling
          bool lTfDropped = (lUniformDist(lGen) <= mPercentageToBuild) ? false : true;
          if (lTfDropped) {
            mDroppedThrottlingStfs.SetEvent(lStfId);
            mDroppedStfs.SetEvent(lStfId);
            mNotScheduledTfsCount++;
            // this will only accurately represent the rate
            DDMON_RATE("tfscheduler", "tf.rejected.tf", lThrottlingRejectedSize);
            lThrottlingRejectedSize = 0;
          }
          mLastThrottledStfId = lStfId;
        }

        if (mDroppedThrottlingStfs.GetEvent(lStfId)) {
          mTfSizeTotalRejected += pStfInfo.stf_size();
          lThrottlingRejectedSize += pStfInfo.stf_size();
          pResponse.set_status(SchedulerStfInfoResponse::DROP_STFS_THROTTLING);
          return;
        }
      }
    }

    // Drop not running
    if (!mRunning) {
      pResponse.set_status(SchedulerStfInfoResponse::DROP_NOT_RUNNING);
      requestDropAllUnlocked(lStfId, lStfSenderId);
      return;
    }
    // DROP When stfsenders are not complete
    if (mConnManager.getStfSenderState() != StfSenderState::STF_SENDER_STATE_OK) {
      pResponse.set_status(SchedulerStfInfoResponse::DROP_STFS_INCOMPLETE);
      requestDropAllUnlocked(lStfId, lStfSenderId);
      return;
    }

    // check for high watermarks
    const auto lUsedBuffer = pStfInfo.stfs_info().buffer_used();
    const auto lTotalBuffer = pStfInfo.stfs_info().buffer_size();
    if (lUsedBuffer > (lTotalBuffer * 95 / 100) ) {
      mMemWatermarkCondition.notify_one();
    }
    // check for buffer overrun, and instruct to drop
    if (lUsedBuffer > (lTotalBuffer * 97 / 100)) {
      requestDropAllUnlocked(lStfId, lStfSenderId);
      WDDLOG_GRL(1000, "addStfInfo: stfs buffer full, dropping stf. stfs_id={} buffer_used={} buffer_size={}",
        lStfSenderId, lUsedBuffer, lTotalBuffer);
      pResponse.set_status(SchedulerStfInfoResponse::DROP_STFS_BUFFER_FULL);
      return;
    }

    // send to aggregation
    mReportedStfInfoQueue.push(std::move(pStfInfo));

    // add the current STF to the list
    pResponse.set_status(SchedulerStfInfoResponse::OK);
  }
}

void TfSchedulerStfInfo::TfCompleterThread()
{
  DDDLOG("Starting TfCompleterThread.");

  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();

  while (mRunning) {
    auto lStfInfoOpt = mReportedStfInfoQueue.pop();
    if (!lStfInfoOpt) {
      continue;
    }
    bool lNewTfCreated = false; // trigger top down update
    auto &lStfInfo = lStfInfoOpt.value();
    const auto lStfId = lStfInfo.stf_id();
    const std::string &lStfSenderId = lStfInfo.process_id();

    { // update max seen stf id per stfsender
      auto &lMaxStfId = mMaxStfIdPerStfSender[lStfSenderId];
      lMaxStfId = std::max(lMaxStfId, lStfId);
    }

    /// Lock, check, insert and schedule
    std::unique_lock lLock(mGlobalStfInfoLock);

    // check if already dropped?
    if (mDroppedStfs.GetEvent(lStfId)) {
      requestDropSingle(lStfId, lStfSenderId);
      continue;
    }

    // check if already built?
    if (mBuiltTfs.GetEvent(lStfId)) {
      EDDLOG_GRL(5000, "addStfInfo: this TimeFrame was already built. stfsender_id={} stf_id={}", lStfSenderId, lStfId);
      requestDropSingle(lStfId, lStfSenderId);
      continue;
    }

    // out of order update
    if (lStfId <= mMaxCompletedTfId) {
      EDDLOG_GRL(10000, "TfScheduler: Delayed or duplicate STF info. stf_id={} current_stf_id={} from_stf_sender={}", lStfId, mLastStfId, lStfSenderId);
      requestDropSingle(lStfId, lStfSenderId);
      continue;
    }

    // mark the highest tf id to schedule
    std::uint64_t lMaxTfIdToSchedule = 0;

    mLastStfId = std::max(mLastStfId, lStfId);

    // get or create a new tf map of Stf updates
    auto &lStfIdMap = mStfInfoMap[lStfId];
    if (lStfIdMap.empty()) {
      // starting a new tf
      mStfInfoStartTimeMap[lStfId] = std::chrono::steady_clock::now();;
      lNewTfCreated = true;
    }

    lStfIdMap.try_emplace(lStfSenderId, std::move(lStfInfo));
    if (lStfIdMap.size() == lNumStfSenders) {
      // schedule up to <lCurrStfId>
      lMaxTfIdToSchedule = std::max(lMaxTfIdToSchedule, lStfId);
    }

    DDMON("tfscheduler", "stf.update.parallel_tf_num", mStfInfoMap.size());

    // try to create proxies if we cannot schedule the current tf
    if (lMaxTfIdToSchedule == 0) {
      {
        // create proxy stfs below if needed
        // start in the reverse order, and stop when there is already a stf present

        for (auto lMapIt = mStfInfoMap.rbegin(); lMapIt != mStfInfoMap.rend(); ++lMapIt) {
          const auto lCurrStfId = lMapIt->first;

          if (lCurrStfId >= lStfId) {
            continue; // don't create proxy stfs for newer timeframes
          }

          assert (lCurrStfId < lStfId);

          if (lMapIt->second.try_emplace(lStfSenderId, StfInfo{StfInfo::ProxyStfInfo{}}).second) {
            mStfInfoIncomplete[lCurrStfId] = true; // mark incomplete if proxy was inserted
            if (lMapIt->second.size() == lNumStfSenders) {
              // schedule up to <lCurrStfId>
              lMaxTfIdToSchedule = std::max(lMaxTfIdToSchedule, lCurrStfId);
            }
          } else {
            // don't check below, as they should be created by the previous STF
#if !defined (NDEBUG)
          for (auto lDebugIt = lMapIt; lDebugIt != mStfInfoMap.rend(); ++lDebugIt) {
            assert (lDebugIt->second.count(lStfSenderId) == 1);
          }
#endif
            break;
          }
        }
      }

      { // fill the rest of stfs from above if a new tf is created
        // if the new TF was just created, add proxy stfs from all senders that reported higher stfs
        if (lNewTfCreated) {
          for (auto &lMaxIdIt : mMaxStfIdPerStfSender) {
            if (lMaxIdIt.second > lStfId) {
              if (lStfIdMap.try_emplace(lMaxIdIt.first, StfInfo{StfInfo::ProxyStfInfo{}}).second) {
                mStfInfoIncomplete[lStfId] = true;
              }
            }
          }
          if (lStfIdMap.size() == lNumStfSenders) {
            // schedule up to <lStfId>
            lMaxTfIdToSchedule = std::max(lMaxTfIdToSchedule, lStfId);
          }
        }
      }
    }

    // if still there is nothing to build check if stale tf should run
    if (lMaxTfIdToSchedule == 0 && (mStfInfoMap.size() > 10)) {
      assert (mStfInfoStartTimeMap.count(mStfInfoMap.begin()->first) > 0);
      const auto lOldestAgeMs = since<std::chrono::milliseconds>(mStfInfoStartTimeMap[mStfInfoMap.begin()->first]);
      if (lOldestAgeMs > mStaleStfTimeoutMs) {
        mStaleCondition.notify_one();
      }
    }

    // schedule all tfs including (lMaxTfIdToSchedule)
    if (lMaxTfIdToSchedule != 0) {
      // schedule all tfs lower and equal to lMaxTfIdToSchedule in asc order
      while (!mStfInfoMap.empty() && (mStfInfoMap.cbegin()->first <= lMaxTfIdToSchedule)) {

        const auto &lSchedTfId = mStfInfoMap.begin()->first;
        // check how long this took to schedule
        assert (mStfInfoStartTimeMap.count(lSchedTfId) > 0);
        const double lTfDurMs = since<std::chrono::milliseconds>(mStfInfoStartTimeMap[lSchedTfId]);

        // move info to the completed queue
        auto lTfInfoNode = mStfInfoMap.extract(mStfInfoMap.begin());
        mStfInfoStartTimeMap.erase(lSchedTfId);
        mMaxCompletedTfId = std::max(mMaxCompletedTfId, lSchedTfId);

        if (mDroppedStfs.GetEvent(lSchedTfId)) {
          EDDLOG_GRL(5000, "TfScheduling: this TimeFrame was already built. tf_id={}", lSchedTfId);
          continue;
        }

        // mark this as built (handled)
        assert (mBuiltTfs.GetEvent(lSchedTfId) == false);
        mBuiltTfs.SetEvent(lSchedTfId);

        const bool lTfComplete = (mStfInfoIncomplete.count(lSchedTfId) > 0) ?
          (!mStfInfoIncomplete[lSchedTfId]) : (lTfInfoNode.mapped().size() == lNumStfSenders);
        mStfInfoIncomplete.erase(lSchedTfId);

        if (lTfComplete) {
          mCompleteTfCount += 1;
          DDMON("tfscheduler", "stf.update.complete_dur_ms", lTfDurMs);
        } else {
          mIncompleteTfCount += 1;
          DDMON("tfscheduler", "stf.update.incomplete_dur_ms", lTfDurMs);
        }

        mCompleteStfsInfoQueue.push(std::make_tuple(lTfComplete, std::move(lTfInfoNode.mapped())));
      }

      DDMON("tfscheduler", "tf.complete.total", mCompleteTfCount);
      DDMON("tfscheduler", "tf.incomplete.total", mIncompleteTfCount);
    }
  }

  DDDLOG("Exiting TfCompleterThread.");
}

/// TOPOLOGY RUN: Stfs global info
void TfSchedulerStfInfo::addTopologyStfInfo(const StfSenderStfInfo &pStfInfo, SchedulerStfInfoResponse &pResponse)
{
  assert (pStfInfo.stf_source() == StfSource::TOPOLOGICAL);
  assert (pStfInfo.stf_source_info_size() == 1);

  const auto &lStfSourceInfo = pStfInfo.stf_source_info()[0]; // only one is valid
  const auto lDataOrigin = std::string(lStfSourceInfo.data_origin());
  const std::uint64_t lSubSpec = lStfSourceInfo.data_subspec();

  DDDLOG_GRL(5000, "Received a topological STF info: {}/{}", lStfSourceInfo.data_origin(), lStfSourceInfo.data_subspec());

  {
    // Drop not running
    if (!mRunning) {
      pResponse.set_status(SchedulerStfInfoResponse::DROP_NOT_RUNNING);
      return;
    }

    // add the current STF to the list
    pResponse.set_status(SchedulerStfInfoResponse::OK);

    auto lTopoStfInfo = std::make_unique<TopoStfInfo>(pStfInfo, lDataOrigin, lSubSpec);
    mTopoStfInfoQueue.push(std::move(lTopoStfInfo));
  }
}

void TfSchedulerStfInfo::TopoSchedulingThread()
{
  DataDistLogger::SetThreadName("TopoSchedulingThread");
  DDDLOG("Starting StfInfo TopoSchedulingThread.");

  std::uint64_t lNumTfScheds = 0;

  // count how many time an FLP was missing STFs
  std::optional<std::unique_ptr<TopoStfInfo>> lStfInfosOpt;

  while ((lStfInfosOpt = mTopoStfInfoQueue.pop()) != std::nullopt) {
    const std::unique_ptr<TopoStfInfo> &lTopoStfInfo = lStfInfosOpt.value();

    const auto lRunNum = lTopoStfInfo->mStfInfo.partition().run_number();
    if (mRunNumber != lRunNum) {
      mRunNumber = lRunNum;
      IDDLOG("NEW RUN NUMBER: {} {}", lTopoStfInfo->mStfInfo.partition().partition_id(), mRunNumber);
      DataDistMonitor::enable_datadist(mRunNumber, lTopoStfInfo->mStfInfo.partition().partition_id());

      // set log run number
      DataDistLogger::sRunNumber = mRunNumber;
      DataDistLogger::sRunNumberStr = std::to_string(mRunNumber);
      impl::DataDistLoggerCtx::InitInfoLogger();
    }

    TfBuildingInformation lRequest;

    const auto lTfId = lTopoStfInfo->stf_id();
    const std::string &lStfSenderId = lTopoStfInfo->process_id();

    // calculate combined STF size
    const std::uint64_t lTfSize = lTopoStfInfo->stf_size();
    (*lRequest.mutable_stf_size_map())[lStfSenderId] = lTfSize;
    lRequest.set_tf_id(lTfId);
    lRequest.set_tf_size(lTfSize);
    lRequest.set_tf_source(StfSource::TOPOLOGICAL);

    const std::string_view lDataOrigin = std::string_view(lTopoStfInfo->mDataOrigin, 3);
    const std::uint64_t lSubSpec = lTopoStfInfo->mSubSpec;

    DDDLOG_RL(1000, "TopoScheduling: origin={} subspec={} total={}", lDataOrigin, lSubSpec, lNumTfScheds);

    bool lScheduleSuccess = false;

    // 1: Get the best TfBuilder
    std::string lTfBuilderId;
    if (mTfBuilderInfo.findTfBuilderForTopoStf(lDataOrigin, lSubSpec, lTfBuilderId /*out*/) ) {
      lNumTfScheds++;
      assert (!lTfBuilderId.empty());

      TfBuilderRpcClient lRpcCli = mConnManager.getTfBuilderRpcClient(lTfBuilderId);
      // finding and getting the client is racy
      if (lRpcCli) {
        BuildTfResponse lResponse;
        // Notify TfBuilder to build the TF
        if (lRpcCli.get().BuildTfRequest(lRequest, lResponse)) {
          switch (lResponse.status()) {
            case BuildTfResponse::OK:
              lScheduleSuccess = true;
              break;
            case BuildTfResponse::ERROR_NOMEM:
              requestDropTopoStf(lTfId, lStfSenderId);
              break;
            case BuildTfResponse::ERROR_NOT_RUNNING:
              EDDLOG_RL(1000, "Scheduling error: selected TfBuilder returned ERROR_NOT_RUNNING. tf_id={}", lTfBuilderId);
              requestDropTopoStf(lTfId, lStfSenderId);
              break;
            default:
              break;
          }
        } else {
          EDDLOG_RL(1000, "TopoScheduling of TF failed. to_tfb_id={} reason=grpc_error", lTfBuilderId);
          WDDLOG_RL(1000, "Removing TfBuilder from scheduling. tfb_id={}", lTfBuilderId);

          lRpcCli.put();

          requestDropTopoStf(lTfId, lStfSenderId);
          mConnManager.removeTfBuilder(lTfBuilderId);
          mTfBuilderInfo.removeReadyTfBuilder(lTfBuilderId);
        }
      } else {
        // TfBuilder was removed in the meantime, e.g. by housekeeping thread because of stale info
        // We drop the current TF as this is not a likely situation
        WDDLOG("Selected TfBuilder is not currently reachable. TF will be dropped. tfb_id={} tf_id={}",
          lTfBuilderId, lTfId);

        requestDropTopoStf(lTfId, lStfSenderId);
        mTfBuilderInfo.removeReadyTfBuilder(lTfBuilderId);
      }
    } else {
      // No candidate for scheduling
      requestDropTopoStf(lTfId, lStfSenderId);
    }


    if (lScheduleSuccess) {
      mScheduledTfs += 1;
      mTfSizeTotalScheduled += lTfSize;
      DDMON_RATE("tfscheduler", "tf.scheduled.tf", lTfSize);
      DDMON("tfscheduler", "tf.scheduled.data_size_total", mTfSizeTotalScheduled);
    } else {
      mTfSizeTotalRejected += lTfSize;
      DDMON_RATE("tfscheduler", "tf.rejected.tf", lTfSize);
      DDMON("tfscheduler", "tf.rejected.data_size_total", mTfSizeTotalRejected);
    }
  }

  DDDLOG("Exiting StfInfo TopoSchedulingThread.");
}


} /* o2::DataDistribution */
