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

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

void TfSchedulerStfInfo::SchedulingThread()
{
  DataDistLogger::SetThreadName("SchedulingThread");
  DDDLOG("Starting StfInfo Scheduling thread.");

  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const std::set<std::string> lStfSenderIdSet = mConnManager.getStfSenderSet();
  std::vector<std::uint64_t> lStfsToErase;
  lStfsToErase.reserve(1000);

  std::uint64_t lNumTfScheds = 0;

  // count how many time an FLP was missing STFs
  std::map<std::string, std::uint64_t> lStfSenderMissingCnt;
  std::optional<std::vector<StfInfo>> lStfInfosOpt;

  // total number of scheduled Tfs
  std::size_t lScheduledTfs = 0;

  // Build or discard
  bool lBuildIncomplete = mDiscoveryConfig->getBoolParam(BuildStaleTfsKey, BuildStaleTfsValue);
  IDDLOG("TfScheduler: Building of incomplete TimeFrames is {}.", lBuildIncomplete ? "enabled" : "disabled");

  while ((lStfInfosOpt = mCompleteStfsInfoQueue.pop()) != std::nullopt) {

    DDMON("tfscheduler", "tf.rejected.total", mNotScheduledTfsCount);
    DDMON("tfscheduler", "tf.scheduled.total", lScheduledTfs);

    const std::vector<StfInfo> &lStfInfos = lStfInfosOpt.value();
    TfBuildingInformation lRequest;

    // check complete stf information
    assert (!lStfInfos.empty());

    const auto lTfId = lStfInfos[0].stf_id();

    // TODO: alow incomplete TFs
    if (lStfInfos.size() != lNumStfSenders) {
      assert(lStfInfos.size() < lNumStfSenders);

      if (lBuildIncomplete) {
        DDMON("tfscheduler", "tf.scheduled.partial.stf_cnt", lStfInfos.size());
      } else {
        WDDLOG_RL(5000, "StaleStfDropThread: TFs have been discarded due to incomplete number of STFs. total={}", mStaleTfCount);
        requestDropAllFromSchedule(lTfId);
        continue;
      }
    }

    // calculate combined STF size
    std::uint64_t lTfSize = 0;
    for (const auto &lStfI : lStfInfos) {
      lTfSize += lStfI.stf_size();
      (*lRequest.mutable_stf_size_map())[lStfI.process_id()] = lStfI.stf_size();
    }
    lRequest.set_tf_id(lTfId);
    lRequest.set_tf_size(lTfSize);
    lRequest.set_tf_source(StfSource::DEFAULT);

    // 1: Get the best TfBuilder candidate
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
              lScheduledTfs++;
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
          EDDLOG("Scheduling of Tf failed. to_tfb_id={} reason=grpc_error", lTfBuilderId);
          EDDLOG("Removing TfBuilder from scheduling. tfb_id={}", lTfBuilderId);

          lRpcCli.put();

          requestDropAllFromSchedule(lTfId);
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
  auto lStaleStfTimeoutMs = std::clamp(mDiscoveryConfig->getUInt64Param(StaleStfTimeoutMsKey, StaleStfTimeoutMsValue),
      std::uint64_t(250), std::uint64_t(60000));

  IDDLOG("StaleCleanupThread: parameter (consul) {}={}", StaleStfTimeoutMsKey, lStaleStfTimeoutMs);

  while (mRunning) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto lNow = std::chrono::steady_clock::now();

    lStaleStfsToComplete.clear();
    // update housekeeping parameters
    lStaleStfTimeoutMs = std::clamp(mDiscoveryConfig->getUInt64Param(StaleStfTimeoutMsKey, StaleStfTimeoutMsValue),
      std::uint64_t(250), std::uint64_t(60000));
    {
      std::unique_lock lLock(mGlobalStfInfoLock);

      // check all available stf information
      for (const auto &lStfIdInfo : mStfInfoMap) {
        auto lStfId = lStfIdInfo.first;
        auto &lStfInfoVec = lStfIdInfo.second;

        if (lStfInfoVec.empty()) { // this should not happen
          EDDLOG_RL(1000, "Discarding TimeFrame with no STF updates. stf_id={} received={} expected={}",
            lStfId, lStfInfoVec.size(), lNumStfSenders);

          lStaleStfsToComplete.push_back(lStfId);
          continue;
        }

        // check and reap
        const auto &lLastStfInfo = lStfInfoVec.back();
        const std::chrono::duration<double, std::milli> lTimeDiff = std::chrono::abs(lLastStfInfo.mUpdateLocalTime - lNow);

        if (lTimeDiff > std::chrono::milliseconds(lStaleStfTimeoutMs)) {
          WDDLOG_RL(10000, "Stale incomplete TimeFrame. stf_id={} received={} expected={} size_update_ms={}",
            lStfId, lStfInfoVec.size(), lNumStfSenders, std::chrono::duration_cast<std::chrono::milliseconds>(lTimeDiff).count());

          DDMON("tfscheduler", "stf.update.stale_dur_ms", lTimeDiff.count());

          // erase the stale stf
          lStaleStfsToComplete.push_back(lStfId);

          // find missing StfSenders
          std::set<std::string> lMissingStfSenders = lStfSenderIdSet;
          for (const auto &lUpdate : lStfInfoVec) {
            lMissingStfSenders.erase(lUpdate.process_id());
          }

          for (const auto &lStf : lMissingStfSenders) {
            lStfSenderMissingCnt[lStf]++;
          }

          DDDLOG_RL(5000, "Missing STFs from StfSender IDs: {}", boost::algorithm::join(lMissingStfSenders, ", "));
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
        mCompleteStfsInfoQueue.push(std::move(lStaleInfoNode.mapped()));
      }
    }

    if (lStaleStfsToComplete.size() > 0) {

      mStaleTfCount += lStaleStfsToComplete.size();
      DDMON("tfscheduler", "tf.rejected.stale_not_completed_stf", mStaleTfCount);

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
    std::string lStfsToFree;

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
          lStfsToFree = lStfs;
        }

        // log problematic ones
        if (lBufUtil > 0.90) {
          WDDLOG_RL(1000, "HighWatermark: buffer utilization too high. stfs_id={} buffer_util={:.4} buffer_size={}",
            lStfs, lBufUtil, lStfsUtil.buffer_size());
        }
      }

      IDDLOG_RL(5000, "HighWatermark: StfSender buffer utilization max_util={:.4} stfs_id={} mean_util={:.4}",
        lBuffUtilMax, lStfsToFree, lBuffUtilMean);

      DDMON("tfscheduler", "stfsender.buffer.max_util", lBuffUtilMax);

      if (lStfsToFree.empty() || lBuffUtilMax <= 0.90) {
        // make sure the info is regenerated fresh after clearing
        mStfSenderInfoMap.clear();
        continue;
      }

      // check if we have incomplete TFs smaller than the currently build one
      // since all StfSenders are required to send updates in order, all incomplete TFs before
      // the current can be discarded
      assert (!lStfsToFree.empty());

      auto lTimeNow = std::chrono::steady_clock::now();

      for (const auto &it : mStfInfoMap) {
        const auto &lStfId = it.first;
        const auto &lStfVec = it.second;

        // limit how much we drop
        if (lStfsToDropSize > (std::uint64_t(1) << 30)) {
          break;
        }

        assert (!lStfVec.empty());

        // check the TF ID
        if (lStfId > mMaxCompletedTfId) {
          break;
        }

        // check if the TF is too recent
        if (!lStfVec.empty() && (lTimeNow - lStfVec[0].mUpdateLocalTime < 1s)) {
          break;
        }

        // check if this will help the problematic stf sender
        const auto &lFoundStfs = std::find_if(lStfVec.begin(), lStfVec.end(),
          [&cMatch = std::as_const(lStfsToFree)](const StfInfo &pI) {
            return (pI.process_id() == cMatch);
          }
        );
        if (lFoundStfs == lStfVec.end()) {
          continue;
        }

        WDDLOG_GRL(1000, "STFUpdateComplete: dropping incomplete TF with lower id than the currently completed one. "
          "drop_tf_id={} complete_tf_id={}", lStfId, mMaxCompletedTfId);

        lStfsToDropSize += lFoundStfs->stf_size();
        lStfsToDrop.push_back(lStfId);
      }
      // make sure the info is regenerated fresh after clearing
      mStfSenderInfoMap.clear();

      // note: must drop while the map is locked
      for (const auto lStfId :lStfsToDrop) {
        requestDropAllLocked(lStfId); // this will remove the element from mStfInfoMap
      }
    }// unlock mGlobalStfInfoLock

    if (lStfsToDropSize > 0) {
      WDDLOG("HighWatermark: cleared dropped STFs from StfSender. stfs_id={} num_stfs_dropped={} dropped_size={}",
        lStfsToFree, lStfsToDrop.size(), lStfsToDropSize);
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
  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const std::uint64_t lRunNumber = pStfInfo.partition().run_number();
  const auto lStfId = pStfInfo.stf_id();

  if (pStfInfo.stf_source() == StfSource::TOPOLOGICAL) {
    assert (pStfInfo.stf_source_info_size() == 1);
    return addTopologyStfInfo(pStfInfo, pResponse);
  }

  {
    std::unique_lock lLock(mGlobalStfInfoLock);

    DDDLOG_GRL(5000, "addStfInfo: stf info received. stf_id={}", lStfId);

    // always record latest stfsender status for high watermark thread
    mStfSenderInfoMap[pStfInfo.info().process_id()] = pStfInfo.stfs_info();

    // check if we need to restart tf counters for a new run
    if (mRunNumber < lRunNumber) {
      DDDLOG("New RunNumber received. run_number={}", lRunNumber);
      // reset internal counters
      reset();
      mRunNumber = lRunNumber;
    } else if (mRunNumber > lRunNumber) {
      EDDLOG_GRL(500, "New RunNumber is smaller than the previous. run_number={} prev_run_number={}", lRunNumber, mRunNumber);
      pResponse.set_status((!mRunning) ? SchedulerStfInfoResponse::DROP_NOT_RUNNING :
        SchedulerStfInfoResponse::DROP_SCHED_DISCARDED);
      return;
    }

    // check if already dropped?
    if (mDroppedStfs.GetEvent(lStfId)) {
      pResponse.set_status((!mRunning) ? SchedulerStfInfoResponse::DROP_NOT_RUNNING :
        SchedulerStfInfoResponse::DROP_SCHED_DISCARDED);
      assert (mStfInfoMap.count(lStfId) == 0);
      return;
    }

    // check if already built?
    if (mBuiltTfs.GetEvent(lStfId)) {
      pResponse.set_status((!mRunning) ? SchedulerStfInfoResponse::DROP_NOT_RUNNING :
        SchedulerStfInfoResponse::DROP_SCHED_DISCARDED);
      assert (mStfInfoMap.count(lStfId) == 0);
      EDDLOG_GRL(500, "addStfInfo: Stf update with ID that is already built. stfs_id={} stf_id={}",
        pStfInfo.info().process_id(), lStfId);
      return;
    }

    // Drop not running
    if (!mRunning) {
      pResponse.set_status(SchedulerStfInfoResponse::DROP_NOT_RUNNING);
      requestDropAllLocked(lStfId);
      return;
    }
    // DROP When stfsenders are not complete
    if (mConnManager.getStfSenderState() != StfSenderState::STF_SENDER_STATE_OK) {
      pResponse.set_status(SchedulerStfInfoResponse::DROP_STFS_INCOMPLETE);
      requestDropAllLocked(lStfId);
      return;
    }

    // check for high watermarks
    // TODO: fix watermarks
    const auto lUsedBuffer = pStfInfo.stfs_info().buffer_used();
    const auto lTotalBuffer = pStfInfo.stfs_info().buffer_size();
    if (lUsedBuffer > (lTotalBuffer * 95 / 100) ) {
      mMemWatermarkCondition.notify_one();
    }
    // check for buffer overrun, and instruct to drop
    if (lUsedBuffer > (lTotalBuffer * 97 / 100)) {
      WDDLOG_GRL(1000, "addStfInfo: stfs buffer full, dropping stf. stfs_id={} buffer_used={} buffer_size={}",
        pStfInfo.info().process_id(), lUsedBuffer, lTotalBuffer);
      pResponse.set_status(SchedulerStfInfoResponse::DROP_STFS_BUFFER_FULL);
      requestDropAllLocked(lStfId);
      return;
    }

    if (lStfId > mLastStfId + 220) { // warn about the future
      WDDLOG_GRL(500, "TfScheduler: Received STFid is much larger than the currently processed TF id."
        " new_stf_id={} current_stf_id={} from_stf_sender={}", lStfId, mLastStfId, pStfInfo.info().process_id());
    }

    // Sanity check for delayed Stf info
    // TODO: define tolerable delay here
    // seq consistency: FLPs are driving streams independently
    const std::uint64_t lMaxDelayTf = sStfDiscardTimeout.count() * 88;
    const std::uint64_t lMinAccept = mLastStfId < lMaxDelayTf ? 0 : (mLastStfId - lMaxDelayTf);

    DDDLOG_GRL(5000, "TfScheduler: Currently accepting STF id range: start_id={} current_id={}",
      lMinAccept, mLastStfId);

    if ((lStfId < lMinAccept) && (mStfInfoMap.count(lStfId) == 0)) {
      WDDLOG_GRL(1000, "TfScheduler: Delayed or duplicate STF info. stf_id={} current_stf_id={} from_stf_sender={}",
        lStfId, mLastStfId, pStfInfo.info().process_id());

      pResponse.set_status(SchedulerStfInfoResponse::DROP_SCHED_DISCARDED);
      requestDropAllLocked(lStfId);
      return;
    }

    mLastStfId = std::max(mLastStfId, lStfId);
    assert (mDroppedStfs.GetEvent(lStfId) == false);

    // get or create a new vector of Stf updates
    auto &lStfIdVector = mStfInfoMap[lStfId];

    if (lStfIdVector.size() == 0) {
      lStfIdVector.reserve(lNumStfSenders);
    }

    // add the current STF to the list
    pResponse.set_status(SchedulerStfInfoResponse::OK);
    const auto lTimeNow = std::chrono::steady_clock::now();
    lStfIdVector.emplace_back(lTimeNow, pStfInfo);

    DDMON("tfscheduler", "stf.update.parallel_tf_num", mStfInfoMap.size());

    // check if complete
    if (lStfIdVector.size() == lNumStfSenders) {
      // NOTE: if we have a completed TF, we have to check if there are other incomplete TFs before the current
      //       All incomplete can be treated as stale and build or discard based on BuildStaleTfsKey config

      // queue all incomplete STFs before the complete one
      while (mStfInfoMap.begin()->first < lStfId) {

        auto &lStaleStfId = mStfInfoMap.begin()->first;
        auto &lStaleStfVector = mStfInfoMap.begin()->second;

        // check how long this was stale
        const std::chrono::duration<double, std::milli> lStaleTfDur = lTimeNow - lStaleStfVector.back().mUpdateLocalTime;

        // move info to the completed queue
        auto lStaleInfoNode = mStfInfoMap.extract(mStfInfoMap.begin());
        mMaxCompletedTfId = std::max(mMaxCompletedTfId, lStaleStfId);

        // mark this as built (handled)
        mBuiltTfs.SetEvent(lStaleStfId);
        mCompleteStfsInfoQueue.push(std::move(lStaleInfoNode.mapped()));

        DDMON("tfscheduler", "stf.update.stale_dur_ms", lStaleTfDur.count());
      }

      // Queue the complete TF
      {
        // Check duration
        const std::chrono::duration<double, std::milli> lTfSchedDur = lTimeNow - lStfIdVector.front().mUpdateLocalTime;

        // move info to the completed queue
        auto lInfoNode = mStfInfoMap.extract(lStfId);
        mMaxCompletedTfId = std::max(mMaxCompletedTfId, lStfId);

        // queue completed TFs
        mBuiltTfs.SetEvent(lStfId);
        mCompleteStfsInfoQueue.push(std::move(lInfoNode.mapped()));

        DDMON("tfscheduler", "stf.update.complete_dur_ms", lTfSchedDur.count());
      }
    }
  }
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

    // 1: Get the best TfBuilder
    std::string lTfBuilderId;
    if (mTfBuilderInfo.findTfBuilderForTopoStf(lDataOrigin, lSubSpec, lTfBuilderId /*out*/) ) {
      lNumTfScheds++;
      assert (!lTfBuilderId.empty());
      // Notify TfBuilder to build the TF
      TfBuilderRpcClient lRpcCli = mConnManager.getTfBuilderRpcClient(lTfBuilderId);

      // finding and getting the client is racy
      if (lRpcCli) {
        BuildTfResponse lResponse;

        if (lRpcCli.get().BuildTfRequest(lRequest, lResponse)) {
          switch (lResponse.status()) {
            case BuildTfResponse::OK:
              break;
            case BuildTfResponse::ERROR_NOMEM:
              requestDropTopoStf(lTfId, lStfSenderId);
              break;
            case BuildTfResponse::ERROR_NOT_RUNNING:
              EDDLOG("Scheduling error: selected TfBuilder returned ERROR_NOT_RUNNING. tf_id={:s}", lTfBuilderId);
              requestDropTopoStf(lTfId, lStfSenderId);
              break;
            default:
              break;
          }
        } else {
          EDDLOG("TopoScheduling of TF failed. to_tfb_id={} reason=grpc_error", lTfBuilderId);
          WDDLOG("Removing TfBuilder from scheduling. tfb_id={}", lTfBuilderId);

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
  }

  DDDLOG("Exiting StfInfo TopoSchedulingThread.");
}


} /* o2::DataDistribution */
