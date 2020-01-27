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

#include <FairMQLogger.h>

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
  LOG(DEBUG) << "Starting StfInfo Scheduling thread...";

  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const std::set<std::string> lStfSenderIdSet = mConnManager.getStfSenderSet();
  std::vector<std::uint64_t> lStfsToErase;
  lStfsToErase.reserve(1000);
  auto lLastReapTime = std::chrono::system_clock::now();

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

          {
            static std::uint64_t sNumTfScheds = 0;
            if (++sNumTfScheds % 50 == 0) {
              LOG(DEBUG) << "Scheduling TF: " << lTfId << " to TfBuilder: " << lTfBuilderId << ", total: " << sNumTfScheds;
            }
          }

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
                case BuildTfResponse::OK :
                  // marked TfBuilder as scheduled
                  mTfBuilderInfo.markTfBuilderWithTfId(lTfBuilderId, lRequest.tf_id());
                  break;
                case BuildTfResponse::ERROR_NOMEM :
                  LOG (ERROR) << "Scheduling error: selected TfBuilder returned ERROR_NOMEM";
                  break;
                case BuildTfResponse::ERROR_NOT_RUNNING :
                  LOG (ERROR) << "Scheduling error: selected TfBuilder returned ERROR_NOT_RUNNING";
                  break;
                default:
                  break;
              }
            } else {
              LOG(ERROR) << "Scheduling TF to TfBuilder " << lTfBuilderId << " failed: gRPC error.";
              LOG(WARNING) << "Removing TfBuilder: " << lTfBuilderId;

              lRpcCli.put();
              mConnManager.dropAllStfsAsync(lTfId);
              mConnManager.removeTfBuilder(lTfBuilderId);
              mTfBuilderInfo.removeReadyTfBuilder(lTfBuilderId);
            }
          } else {
            // TfBuilder was removed in the meantime, e.g. by housekeeping thread because of stale info
            // We drop the current TF as this is not a likely situation
            LOG (WARNING) << "Selected TfBuilder is not reachable for TF: " << lTfId
                          << ", TfBuilded id: " << lTfBuilderId;

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

    if (lNow - lLastReapTime  > sStfReapTime) {
      lLastReapTime = lNow;

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
          if (lTimeDiff > sStfReapTime) {
            LOG(WARNING) << "Reaping SubTimeFrames with ID: " << lStfId
                         << ". Received STF infos: "
                         << lStfInfoVec.size() << " / " << lNumStfSenders;

            // find missing StfSenders
            std::set<std::string> lMissingStfSenders = lStfSenderIdSet;

            for (const auto &lUpdate : lStfInfoVec) {
              lMissingStfSenders.erase(lUpdate.process_id());
            }

            std::string lMissingIds = boost::algorithm::join(lMissingStfSenders, ", ");
            // for (const auto &lMissingId : lMissingStfSenders) {
            //   lMissingIds += lMissingId;
            // }

            LOG(WARNING) << "  Missing StfSender IDs: " << lMissingIds;

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
        LOG(WARNING) << "SchedulingThread: Number of reaped STFs:" << lStfsToErase.size();
      }
    }

    // wait for notification or reap on timeout
    {
      std::unique_lock lLock(mCompleteStfInfoLock);
      if (! mCompleteStfsInfo.empty()) {
        continue;
      }

      if (std::cv_status::timeout == mStfScheduleCondition.wait_for(lLock, sStfReapTime)) {
        LOG(INFO) << "No new completed SubTimeFrame updates in " << sStfReapTime.count() << "s. Starting StfInfo reap procedure...";
      }
    }
  }

  LOG(DEBUG) << "Exiting StfInfo Scheduling thread...";
}

void TfSchedulerStfInfo::addAddStfInfo(const StfSenderStfInfo &pStfInfo, SchedulerStfInfoResponse &pResponse)
{
  const auto lNumStfSenders = mDiscoveryConfig->status().stf_sender_count();
  const auto lStfId = pStfInfo.stf_id();

  if (!mRunning) {
    pResponse.set_status(SchedulerStfInfoResponse::DROP_NOT_RUNNING);
    return;
  }

  {
    std::unique_lock lLock(mGlobalStfInfoLock);

    if (lStfId > mLastStfId + 100) {
      LOG(DEBUG) << "addAddStfInfo: STF id much larger than the current TF id: (" << lStfId << ") > (" << mLastStfId << ") from: " << pStfInfo.info().process_id();
    }

    // Sanity check for delayed Stf info
    // TODO: define tolerable delay here
    const std::uint64_t lMaxDelayTf = sStfReapTime.count() * 44;
    const auto lMinAccept = mLastStfId < lMaxDelayTf ? 0 : mLastStfId - lMaxDelayTf;

    if ((lStfId < lMinAccept) && (mStfInfoMap.count(lStfId) == 0)) {
      LOG(WARNING) << "Delayed or duplicate STF info for STF_id: " << lStfId
                   << " from StfBuilder: " << pStfInfo.info().process_id()
                   << ". Currently processing STF_id: " << mLastStfId;

      // TODO: reaped or scheduled?
      // pResponse.set_status(SchedulerStfInfoResponse::DROP_SCHED_REAPED);
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
