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
  auto lLastDiscardTime = std::chrono::system_clock::now();

  std::uint64_t lNumTfScheds = 0;

  // count how many time an FLP was missing STFs
  std::map<std::string, std::uint64_t> lStfSenderMissingCnt;

  std::vector<StfInfo> lStfInfos;

  while (mRunning) {

    {
      {
        std::unique_lock lLock(mCompleteStfInfoLock);

        if (mCompleteStfsInfo.empty()) {
          lStfInfos.clear();
        } else {
          lStfInfos = std::move(mCompleteStfsInfo.front());
          mCompleteStfsInfo.pop_front();
        }
      }

      if (!lStfInfos.empty()) {
        // check complete stf information
        assert(lStfInfos.size() == lNumStfSenders);

        // calculate combined STF size
        const std::uint64_t lTfSize = std::accumulate(lStfInfos.begin(), lStfInfos.end(),
          0, [&](std::uint64_t pSum, const StfInfo &pElem) {
            return pSum + pElem.stf_size();
          }
        );

        const auto lTfId = lStfInfos[0].stf_id();

        // 1: Get the best TfBuilder candidate
        std::string lTfBuilderId;
        if (mTfBuilderInfo.findTfBuilderForTf(lTfSize, lTfBuilderId /*out*/) ) {
          lNumTfScheds++;
          DDLOGF_RL(1000, DataDistSeverity::debug, "Scheduling new TF. tf_id={} tfb_id={} total={}",
            lTfId, lTfBuilderId, lNumTfScheds);

          assert (!lTfBuilderId.empty());
          // Notify TfBuilder to build the TF
          TfBuilderRpcClient lRpcCli = mConnManager.getTfBuilderRpcClient(lTfBuilderId);

          // finding and getting the client is racy
          if (lRpcCli) {
            TfBuildingInformation lRequest;
            BuildTfResponse lResponse;

            lRequest.set_tf_id(lTfId);
            lRequest.set_tf_size(lTfSize);
            for (const auto &lStfI : lStfInfos) {
              (*lRequest.mutable_stf_size_map())[lStfI.process_id()] = lStfI.stf_size();
            }

            if (lRpcCli.get().BuildTfRequest(lRequest, lResponse)) {
              switch (lResponse.status()) {
                case BuildTfResponse::OK:
                  // marked TfBuilder as scheduled
                  mTfBuilderInfo.markTfBuilderWithTfId(lTfBuilderId, lRequest.tf_id());
                  break;
                case BuildTfResponse::ERROR_NOMEM:
                  EDDLOG("Scheduling error: selected TfBuilder returned ERROR_NOMEM. tf_id={:s}", lTfBuilderId);
                  break;
                case BuildTfResponse::ERROR_NOT_RUNNING:
                  EDDLOG("Scheduling error: selected TfBuilder returned ERROR_NOT_RUNNING. tf_id={:s}", lTfBuilderId);
                  break;
                default:
                  break;
              }
            } else {
              EDDLOG("Scheduling of TF failed. to_tfb_id={:s} reason=grpc_error", lTfBuilderId);
              WDDLOG("Removing TfBuilder from scheduling. tfb_id={:s}", lTfBuilderId);

              lRpcCli.put();
              requestDropAll(lTfId);
              mConnManager.removeTfBuilder(lTfBuilderId);
              mTfBuilderInfo.removeReadyTfBuilder(lTfBuilderId);
            }
          } else {
            // TfBuilder was removed in the meantime, e.g. by housekeeping thread because of stale info
            // We drop the current TF as this is not a likely situation
            WDDLOG("Selected TfBuilder is not currently reachable. TF will be dropped. tfb_id={} tf_id={}",
              lTfBuilderId, lTfId);

            requestDropAll(lTfId);
            // mConnManager.removeTfBuilder(lTfBuilderId);
            // mTfBuilderInfo.removeReadyTfBuilder(lTfBuilderId);
          }
        } else {
          // No candidate for scheduling
          requestDropAll(lTfId);
        }
      }
    }

    const auto lNow = std::chrono::system_clock::now();

    if (lNow - lLastDiscardTime > sStfDiscardTimeout) {
      lLastDiscardTime = lNow;

      lStfsToErase.clear();
      {
        std::unique_lock lLock(mGlobalStfInfoLock);

        // check all available stf information
        for (const auto &lStfIdInfo : mStfInfoMap) {
          auto lStfId = lStfIdInfo.first;
          auto &lStfInfoVec = lStfIdInfo.second;

          assert(!lStfInfoVec.empty());

          // check reap
          const auto &lLastStfInfo = lStfInfoVec.back();
          const auto lTimeDiff = std::chrono::abs(lLastStfInfo.mUpdateLocalTime - lNow);
          if (lTimeDiff > sStfDiscardTimeout) {
            WDDLOG("Discarding incomplete SubTimeFrame. stf_id={:d} received={:d} expected={:d}",
              lStfId, lStfInfoVec.size(), lNumStfSenders);

            // find missing StfSenders
            std::set<std::string> lMissingStfSenders = lStfSenderIdSet;

            for (const auto &lUpdate : lStfInfoVec) {
              lMissingStfSenders.erase(lUpdate.process_id());
            }

            std::string lMissingIds = boost::algorithm::join(lMissingStfSenders, ", ");
            DDDLOG("Missing STFs from StfSender IDs: {:s}", lMissingIds);

            for (const auto &lStf : lMissingStfSenders) {
              lStfSenderMissingCnt[lStf]++;
              DDDLOG("Missing STF ids: stfs_id={} missing_cnt={}", lStf, lStfSenderMissingCnt[lStf]);
            }

            // TODO: more robust reporting needed!
            //       which stfSenders did not send?

            lStfsToErase.push_back(lStfId);
            requestDropAll(lStfId);
          }
        }
      }

      if (lStfsToErase.size() > 0) {
        WDDLOG("SchedulingThread: TFs have been discarded due to incomplete number of STFs. discarded_tf_count={}",
          lStfsToErase.size());
      }
    }

    // wait for notification or discard on timeout
    {
      std::unique_lock lLock(mCompleteStfInfoLock);
      if (! mCompleteStfsInfo.empty()) {
        continue;
      }

      if (std::cv_status::timeout == mStfScheduleCondition.wait_for(lLock, sStfDiscardTimeout)) {
        WDDLOG("No new complete SubTimeFrames in {:d} seconds. Starting stale SubTimeFrame discard procedure.",
          sStfDiscardTimeout.count());
      }
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

      {
        std::unique_lock lLock(mGlobalStfInfoLock);
        mStfInfoMap.erase(lStfId);
      }

      mConnManager.dropAllStfsAsync(lStfId);
    }

  }
  DDDLOG("Exiting StfInfo Scheduler stf drop thread.");
}

void TfSchedulerStfInfo::addStfInfo(const StfSenderStfInfo &pStfInfo, SchedulerStfInfoResponse &pResponse)
{
  static std::uint64_t sLastDropNotRunning = -1;
  static std::uint64_t sLastDropIncomplete = -1;

  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const auto lStfId = pStfInfo.stf_id();

  // catch all response
  pResponse.set_status(SchedulerStfInfoResponse::DROP_SCHED_DISCARDED);

  // Drop not running
  if ((sLastDropNotRunning == lStfId) || !mRunning) {
    pResponse.set_status(SchedulerStfInfoResponse::DROP_NOT_RUNNING);
    sLastDropNotRunning = lStfId;
    return;
  }

  // DROP When not complete
  if ((sLastDropIncomplete == lStfId) || (mConnManager.getStfSenderState() != StfSenderState::STF_SENDER_STATE_OK)) {
    sLastDropIncomplete = lStfId;
    pResponse.set_status(SchedulerStfInfoResponse::DROP_STFS_INCOMPLETE);

    // drop the remaining of this STFs if we transitioned to running
    if (mConnManager.getStfSenderState() == StfSenderState::STF_SENDER_STATE_OK) {
      requestDropAll(lStfId);
    }
    return;
  }

  {
    std::unique_lock lLock(mGlobalStfInfoLock);

    if (lStfId > mLastStfId + 220) { // 5s into the future
      WDDLOG_RL(500, "TfScheduler: Received STFid is much larger than the currently processed TF id."
        " new_stf_id={} current_stf_id={} from_stf_sender={}", lStfId, mLastStfId, pStfInfo.info().process_id());

      // we only accept these if mStfInfoMap is empty. Meaning the run was re-started.
      if (!mStfInfoMap.empty()) {
        pResponse.set_status(SchedulerStfInfoResponse::DROP_SCHED_DISCARDED);
        requestDropAll(lStfId); // drop globally just in case other FLPs have it
        return;
      }
    }

    // Sanity check for delayed Stf info
    // TODO: define tolerable delay here
    const std::uint64_t lMaxDelayTf = sStfDiscardTimeout.count() * 44;
    const std::uint64_t lMinAccept = mLastStfId < lMaxDelayTf ? 0 : (mLastStfId - lMaxDelayTf);

    DDDLOG_GRL(5000, "TfScheduler: Currently accepting time frame ids. start_id={} current_id={}",
      lMinAccept, mLastStfId);

    if ((lStfId < lMinAccept) && (mStfInfoMap.count(lStfId) == 0)) {
      WDDLOG_RL(1000, "TfScheduler: Delayed or duplicate STF info. stf_id={} current_stf_id={} from_stf_sender={}",
        lStfId, mLastStfId, pStfInfo.info().process_id());

      // TODO: discarded or scheduled?
      pResponse.set_status(SchedulerStfInfoResponse::DROP_SCHED_DISCARDED);
      requestDropAll(lStfId);
      return;
    }

    mLastStfId = std::max(mLastStfId, lStfId);

    // get or create a new vector of Stf updates
    auto &lStfIdVector = mStfInfoMap[lStfId];

    if (lStfIdVector.size() == 0) {
      lStfIdVector.reserve(lNumStfSenders);
    }

    lStfIdVector.emplace_back(
      std::chrono::system_clock::now(),
      pStfInfo
    );

    pResponse.set_status(SchedulerStfInfoResponse::OK);

    // check if complete
    if (lStfIdVector.size() == lNumStfSenders) {

      auto lInfo = mStfInfoMap.extract(lStfId);
      lLock.unlock();

      {
        std::unique_lock lLockComplete(mCompleteStfInfoLock);
        mCompleteStfsInfo.emplace_back(std::move(lInfo.mapped()));
      }

      mStfScheduleCondition.notify_one();
    }
  }
}


}
} /* o2::DataDistribution */
