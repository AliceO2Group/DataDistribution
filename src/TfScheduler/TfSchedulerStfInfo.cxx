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
  DDLOGF(fair::Severity::DEBUG, "Starting StfInfo Scheduling thread.");

  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const std::set<std::string> lStfSenderIdSet = mConnManager.getStfSenderSet();
  std::vector<std::uint64_t> lStfsToErase;
  lStfsToErase.reserve(1000);
  auto lLastDiscardTime = std::chrono::system_clock::now();
  std::uint64_t lNumTfScheds = 0;

  while (mRunning) {

    {
      std::vector<StfInfo> lStfInfos;

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
        if ( mTfBuilderInfo.findTfBuilderForTf(lTfSize, lTfBuilderId /*out*/) ) {
          lNumTfScheds++;
          DDLOGF_RL(1000, fair::Severity::DEBUG, "Scheduling new TF. tf_id={:d} tfb_id={:s} total={:d}",
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
                  DDLOGF(fair::Severity::ERROR,
                    "Scheduling error: selected TfBuilder returned ERROR_NOMEM. tf_id={:s}", lTfBuilderId);
                  break;
                case BuildTfResponse::ERROR_NOT_RUNNING:
                  DDLOGF(fair::Severity::ERROR,
                    "Scheduling error: selected TfBuilder returned ERROR_NOT_RUNNING. tf_id={:s}", lTfBuilderId);
                  break;
                default:
                  break;
              }
            } else {
              DDLOGF(fair::Severity::ERROR, "Scheduling of TF failed. to_tfb_id={:s} reason=grpc_error", lTfBuilderId);
              DDLOGF(fair::Severity::WARNING, "Removing TfBuilder from scheduling. tfb_id={:s}", lTfBuilderId);

              lRpcCli.put();
              mConnManager.dropAllStfsAsync(lTfId);
              mConnManager.removeTfBuilder(lTfBuilderId);
              mTfBuilderInfo.removeReadyTfBuilder(lTfBuilderId);
            }
          } else {
            // TfBuilder was removed in the meantime, e.g. by housekeeping thread because of stale info
            // We drop the current TF as this is not a likely situation
            DDLOGF(fair::Severity::WARNING,
              "Selected TfBuilder is not currently reachable. TF will be dropped. tfb_id={:s} tf_id={:d}",
              lTfBuilderId, lTfId);

            mConnManager.dropAllStfsAsync(lTfId);
            // mConnManager.removeTfBuilder(lTfBuilderId);
            // mTfBuilderInfo.removeReadyTfBuilder(lTfBuilderId);
          }
        } else {
          // No candidate for scheduling
          mConnManager.dropAllStfsAsync(lTfId);
        }
      }
    }

    const auto lNow = std::chrono::system_clock::now();

    if (lNow - lLastDiscardTime  > sStfDiscardTimeout) {
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
            DDLOGF(fair::Severity::WARNING,
              "Discarding incomplete SubTimeFrame. stf_id={:d} received={:d} expected={:d}",
              lStfId, lStfInfoVec.size(), lNumStfSenders);

            // find missing StfSenders
            std::set<std::string> lMissingStfSenders = lStfSenderIdSet;

            for (const auto &lUpdate : lStfInfoVec) {
              lMissingStfSenders.erase(lUpdate.process_id());
            }

            std::string lMissingIds = boost::algorithm::join(lMissingStfSenders, ", ");
            DDLOGF(fair::Severity::DEBUG, "Missing STFs from StfSender IDs: {:s}", lMissingIds);

            // TODO: more robust reporting needed!
            //       which stfSenders did not send?

            lStfsToErase.push_back(lStfId);
          }
        }

        for (const auto &lStfId : lStfsToErase) {
          mConnManager.dropAllStfsAsync(lStfId);
          mStfInfoMap.erase(lStfId);
        }
      }

      if (lStfsToErase.size() > 0) {
        DDLOGF(fair::Severity::WARNING,
          "TFs have been discarded due to incomplete number of STFs. discarded_tf_count={:d}",
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
        DDLOGF(fair::Severity::WARNING,
          "No new complete SubTimeFrames in {:d} seconds. Starting stale SubTimeFrame discard procedure.",
          sStfDiscardTimeout.count());
      }
    }
  }

  DDLOGF(fair::Severity::DEBUG, "Exiting StfInfo Scheduling thread.");
}

void TfSchedulerStfInfo::addStfInfo(const StfSenderStfInfo &pStfInfo, SchedulerStfInfoResponse &pResponse)
{
  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const auto lStfId = pStfInfo.stf_id();

  if (!mRunning) {
    pResponse.set_status(SchedulerStfInfoResponse::DROP_NOT_RUNNING);
    return;
  }

  {
    std::unique_lock lLock(mGlobalStfInfoLock);

    if (lStfId > mLastStfId + 200) {
      DDLOGF(fair::Severity::DEBUG,
        "Received STFid is much larger than the currently processed TF id. new_stf_id={} current_stf_id={} from_stf_sender={}",
        lStfId, mLastStfId, pStfInfo.info().process_id()
      );
    }

    // Sanity check for delayed Stf info
    // TODO: define tolerable delay here
    const std::uint64_t lMaxDelayTf = sStfDiscardTimeout.count() * 44;
    const auto lMinAccept = mLastStfId < lMaxDelayTf ? 0 : mLastStfId - lMaxDelayTf;

    if ((lStfId < lMinAccept) && (mStfInfoMap.count(lStfId) == 0)) {
      DDLOGF(fair::Severity::WARNING, "Delayed or duplicate STF info. "
        "stf_id={:d} current_stf_id={:d} from_stf_sender={:s}",
        lStfId, mLastStfId, pStfInfo.info().process_id());

      // TODO: discarded or scheduled?
      // pResponse.set_status(SchedulerStfInfoResponse::DROP_SCHED_DISCARDED);
      // return;
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
