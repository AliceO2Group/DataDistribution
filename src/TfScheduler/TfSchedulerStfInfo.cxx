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

#include <boost/algorithm/string/join.hpp>

#include <set>
#include <tuple>
#include <algorithm>

namespace o2
{
namespace DataDistribution
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

  while ((lStfInfosOpt = mCompleteStfsInfoQueue.pop()) != std::nullopt) {
    const std::vector<StfInfo> &lStfInfos = lStfInfosOpt.value();
    TfBuildingInformation lRequest;

    // check complete stf information
    assert (!lStfInfos.empty());

    const auto lTfId = lStfInfos[0].stf_id();

    // TODO: alow incomplete TFs
    if (lStfInfos.size() != lNumStfSenders) {
      requestDropAllFromSchedule(lTfId);
      continue;
    }

    // calculate combined STF size
    std::uint64_t lTfSize = 0;
    for (const auto &lStfI : lStfInfos) {
      lTfSize += lStfI.stf_size();
      (*lRequest.mutable_stf_size_map())[lStfI.process_id()] = lStfI.stf_size();
    }
    lRequest.set_tf_id(lTfId);
    lRequest.set_tf_size(lTfSize);

    // 1: Get the best TfBuilder candidate
    std::string lTfBuilderId;
    if (mTfBuilderInfo.findTfBuilderForTf(lTfSize, lTfBuilderId /*out*/) ) {
      lNumTfScheds++;
      DDDLOG_RL(1000, "Scheduling new TF. tf_id={} tfb_id={} total={}", lTfId, lTfBuilderId, lNumTfScheds);

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
              mTfBuilderInfo.markTfBuilderWithTfId(lTfBuilderId, lRequest.tf_id());
              break;
            case BuildTfResponse::ERROR_NOMEM:
              EDDLOG("Scheduling error: selected TfBuilder returned ERROR_NOMEM. tf_id={:s}", lTfBuilderId);
              requestDropAllFromSchedule(lTfId);
              break;
            case BuildTfResponse::ERROR_NOT_RUNNING:
              EDDLOG("Scheduling error: selected TfBuilder returned ERROR_NOT_RUNNING. tf_id={:s}", lTfBuilderId);
              requestDropAllFromSchedule(lTfId);
              break;
            default:
              break;
          }
        } else {
          EDDLOG("Scheduling of TF failed. to_tfb_id={:s} reason=grpc_error", lTfBuilderId);
          WDDLOG("Removing TfBuilder from scheduling. tfb_id={:s}", lTfBuilderId);

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
  std::vector<std::uint64_t> lStfsToErase;
  auto lLastDiscardTime = std::chrono::steady_clock::now();

  // count how many time an FLP was missing STFs
  std::map<std::string, std::uint64_t> lStfSenderMissingCnt;

  std::vector<StfInfo> lStfInfos;

  while (mRunning) {
    std::this_thread::sleep_for(std::chrono::seconds(sStfDiscardTimeout));
    lLastDiscardTime = std::chrono::steady_clock::now();

    lStfsToErase.clear();
    {
      std::unique_lock lLock(mGlobalStfInfoLock);

      // check all available stf information
      for (const auto &lStfIdInfo : mStfInfoMap) {
        auto lStfId = lStfIdInfo.first;
        auto &lStfInfoVec = lStfIdInfo.second;

        if (lStfInfoVec.empty()) { // this should not happen
          EDDLOG_RL(1000, "Discarding TimeFrame with no STF updates. stf_id={} received={} expected={}",
            lStfId, lStfInfoVec.size(), lNumStfSenders);

          lStfsToErase.push_back(lStfId);
          continue;
        }

        // check reap
        const auto &lLastStfInfo = lStfInfoVec.back();
        const auto lTimeDiff = std::chrono::abs(lLastStfInfo.mUpdateLocalTime - lLastDiscardTime);
        if (lTimeDiff > sStfDiscardTimeout) {
          WDDLOG_RL(1000, "Discarding incomplete SubTimeFrame. stf_id={} received={} expected={}",
            lStfId, lStfInfoVec.size(), lNumStfSenders);

          // find missing StfSenders
          std::set<std::string> lMissingStfSenders = lStfSenderIdSet;

          for (const auto &lUpdate : lStfInfoVec) {
            lMissingStfSenders.erase(lUpdate.process_id());
          }

          std::string lMissingIds = boost::algorithm::join(lMissingStfSenders, ", ");
          DDDLOG("Missing STFs from StfSender IDs: {}", lMissingIds);

          for (const auto &lStf : lMissingStfSenders) {
            lStfSenderMissingCnt[lStf]++;
          }

          lStfsToErase.push_back(lStfId);
        }
      }

      // drop outside of the main iteration loop
      for(const auto &lStfIdToDrop : lStfsToErase) {
        requestDropAllLocked(lStfIdToDrop);
      }
    }

    if (lStfsToErase.size() > 0) {
      WDDLOG("SchedulingThread: TFs have been discarded due to incomplete number of STFs. discarded_tf_count={}",
        lStfsToErase.size());

      for (const auto &lStfSenderCnt : lStfSenderMissingCnt) {
        if (lStfSenderCnt.second > 0) {
          DDDLOG("StfSender with missing ids: stfsender_id={} missing_cnt={}",
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
        if (lBufUtil > 0.95) {
          WDDLOG_RL(1000, "HighWatermark: buffer utilization too high. stfs_id={} buffer_util={:.4} buffer_size={}",
            lStfs, lBufUtil, lStfsUtil.buffer_size());
        }
      }

      IDDLOG_RL(5000, "HighWatermark: StfSender buffer utilization max_util={:.4} stfs_id={} mean_util={:.4}",
        lBuffUtilMax, lStfsToFree, lBuffUtilMean);

      if (lBuffUtilMax <= 0.95) {
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

        // check the TFID
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
      IDDLOG("HighWatermark: cleared dropped STFs from StfSender. stfs_id={} num_stfs_dropped={} dropped_size={}",
        lStfsToFree, lStfsToDrop.size(), lStfsToDropSize);
    }
  }

  DDDLOG("Exiting StfInfo Scheduling thread.");
}


void TfSchedulerStfInfo::DropThread()
{
  while (mRunning) {

    std::tuple<std::uint64_t> lStfToDrop;

    while (mDropQueue.pop(lStfToDrop)) {

      const std::uint64_t lStfId = std::get<0>(lStfToDrop);

#if !defined(NDEBUG)
      { // Should remove earlier?
        std::unique_lock lLock(mGlobalStfInfoLock);
        assert (mStfInfoMap.count(lStfId) == 0);
      }
#endif
      mConnManager.dropAllStfsAsync(lStfId);
    }
  }

  DDDLOG("Exiting StfInfo Scheduler stf drop thread.");
}

void TfSchedulerStfInfo::addStfInfo(const StfSenderStfInfo &pStfInfo, SchedulerStfInfoResponse &pResponse)
{
  static std::chrono::duration<double, std::milli> sCompleteTfDurAvg = std::chrono::duration<double, std::milli>(0);
  static std::chrono::duration<double, std::milli> sCompleteTfDurMax = std::chrono::duration<double, std::milli>(0);

  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const auto lStfId = pStfInfo.stf_id();

  {
    std::unique_lock lLock(mGlobalStfInfoLock);

    // always record latest stfsender status for high watermark thread
    mStfSenderInfoMap[pStfInfo.info().process_id()] = pStfInfo.stfs_info();

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
    if (lUsedBuffer > (lTotalBuffer * 99 / 100)) {
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
    const std::uint64_t lMaxDelayTf = sStfDiscardTimeout.count() * 44;
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
    lStfIdVector.emplace_back(std::chrono::steady_clock::now(), pStfInfo);

    // check if complete
    if (lStfIdVector.size() == lNumStfSenders) {
      { // check duration
        const std::chrono::duration<double, std::milli> lTfSchedDur = lStfIdVector.rbegin()->mUpdateLocalTime -
          lStfIdVector.begin()->mUpdateLocalTime;

        sCompleteTfDurAvg += (lTfSchedDur / 16 - sCompleteTfDurAvg / 16);
        sCompleteTfDurMax = std::max(lTfSchedDur, sCompleteTfDurMax);

        DDDLOG_GRL(1000, "STFUpdateComplete: collected {} STFs. current_dur_ms={:.3} mean_dur_ms={:.3} max_dur_ms={:.3}",
          lNumStfSenders, lTfSchedDur.count(), sCompleteTfDurAvg.count(), sCompleteTfDurMax.count());
      }

      auto lInfoNode = mStfInfoMap.extract(lStfId);
      mMaxCompletedTfId = std::max(mMaxCompletedTfId, lStfId);

      // queue completed TFs
      mBuiltTfs.SetEvent(lStfId);
      mCompleteStfsInfoQueue.push(std::move(lInfoNode.mapped()));

    }
  }
}

}
} /* o2::DataDistribution */
