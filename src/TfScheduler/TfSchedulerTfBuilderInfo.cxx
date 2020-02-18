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

#include "TfSchedulerTfBuilderInfo.h"
#include "TfSchedulerInstanceRpc.h"

#include <StfSenderRpcClient.h>

#include <FairMQLogger.h>

#include <set>
#include <tuple>
#include <algorithm>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

void TfSchedulerTfBuilderInfo::updateTfBuilderInfo(const TfBuilderUpdateMessage &pTfBuilderUpdate)
{
  using namespace std::chrono_literals;
  const auto lLocalTime = std::chrono::system_clock::now();

  // recreate timepoint from the received millisecond time stamp
  const std::chrono::milliseconds lUpdateDuration(pTfBuilderUpdate.info().last_update_t());
  const std::chrono::time_point<std::chrono::system_clock> lUpdateTimepoint(lUpdateDuration);

  // check for system time drifts; account for gRPC latency
  const auto lTimeDiff = lLocalTime - lUpdateTimepoint;
  if (lTimeDiff < 0s || lTimeDiff > 1s) {
    LOG(WARNING) << "Large system clock drift detected: "
                 << std::chrono::duration_cast<std::chrono::milliseconds>(lTimeDiff).count()
                 << " ms. Check the time synchronization in the cluster (NTP)!";
  }

  {
    // lock the global info
    std::scoped_lock lLock(mGlobalInfoLock);

    const auto &lTfBuilderId = pTfBuilderUpdate.info().process_id();

    // check if should remove
    if (pTfBuilderUpdate.state() == TfBuilderUpdateMessage::NOT_RUNNING) {

      const auto &lTfIter = mGlobalInfo.find(lTfBuilderId);
      if (lTfIter != mGlobalInfo.end()) {
        // remove from available
        removeReadyTfBuilder(lTfBuilderId);
        // remove from global
        mGlobalInfo.erase(lTfIter);
        LOG(INFO) << "TfBuilder left: " << lTfBuilderId;
      }
      return;
    }

    if (0 == mGlobalInfo.count(lTfBuilderId)) {
      // new info, insert it
      mGlobalInfo.try_emplace(
        lTfBuilderId,
          std::make_shared<TfBuilderInfo>(lLocalTime, pTfBuilderUpdate)
      );
      addReadyTfBuilder(mGlobalInfo.at(lTfBuilderId));

      LOG(INFO) << "TfBuilder joined: " << lTfBuilderId;
    } else {
      auto &lInfo = mGlobalInfo.at(lTfBuilderId);

      // acquire the ready lock, since the data is shared
      std::scoped_lock lLockReady(mReadyInfoLock);
      lInfo->mUpdateLocalTime = lLocalTime;

      // update only when the last scheduled tf is built!
      if (pTfBuilderUpdate.last_built_tf_id() == lInfo->last_scheduled_tf_id()) {
        // store the new information
        lInfo->mTfBuilderUpdate = pTfBuilderUpdate;

        // verify the memory estimation is correct
        if (lInfo->mEstimatedFreeMemory > pTfBuilderUpdate.free_memory() ) {
          LOG (WARNING) << "TfBuilder memory estimate is too high: "
                        << double(lInfo->mEstimatedFreeMemory) / double(pTfBuilderUpdate.free_memory());
        }

        lInfo->mEstimatedFreeMemory = pTfBuilderUpdate.free_memory();

      } else if (pTfBuilderUpdate.last_built_tf_id() < lInfo->last_scheduled_tf_id()) {

        // update scheduler's estimate to be on the safe side
        if (lInfo->mEstimatedFreeMemory > pTfBuilderUpdate.free_memory() ) {

          LOG(DEBUG) << "Ignoring TfBuilder info: last_build < last_scheduled, fixing estimate ratio: "
                     << double(lInfo->mEstimatedFreeMemory) / double(pTfBuilderUpdate.free_memory());

          lInfo->mEstimatedFreeMemory = pTfBuilderUpdate.free_memory();

        } else {
          // if (last_build > last_scheduled)
          // NOTE: there is a "race" between notifying the EPN to build and updating last_scheduled_tf_id
          // in our record. Thus, this codepath is possible, and we should update the est memory since we
          // hold the lock
          lInfo->mEstimatedFreeMemory = std::min(
            lInfo->mEstimatedFreeMemory.load(),
            pTfBuilderUpdate.free_memory()
          );
        }
      }
    }
  } // mGlobalInfoLock unlock
}

void TfSchedulerTfBuilderInfo::HousekeepingThread()
{
  using namespace std::chrono_literals;
  LOG(DEBUG) << "Starting TfBuilderInfo-Housekeeping thread...";

  std::vector<std::string> lIdsToErase;

  while (mRunning) {

    std::this_thread::sleep_for(1000ms);

    {
      std::scoped_lock lLock(mGlobalInfoLock);

      const auto lNow = std::chrono::system_clock::now();

      // reap stale TfBuilders
      assert (lIdsToErase.empty());
      for (const auto &lIdInfo : mGlobalInfo) {
        const auto &lInfo = lIdInfo.second;
        const auto lTimeDiff = std::chrono::abs(lNow - lInfo->mUpdateLocalTime);
        if (lTimeDiff >= sTfBuilderReapTime) {
          lIdsToErase.emplace_back(lInfo->mTfBuilderUpdate.info().process_id());
        }

        LOG(DEBUG) << "TfBuilder info id=" << lInfo->mTfBuilderUpdate.info().process_id()
                   << " free_mem=" << lInfo->mTfBuilderUpdate.free_memory()
                   << " num_buffered_tfs=" << lInfo->mTfBuilderUpdate.num_buffered_tfs();
      }

      if (!lIdsToErase.empty()) {
        for (const auto &lId : lIdsToErase) {
          mGlobalInfo.erase(lId);
          removeReadyTfBuilder(lId);
          LOG (INFO) << "TfBuilder removed from scheduling (stale info), id=" << lId;
        }
        lIdsToErase.clear();
      }
    } // mGlobalInfoLock unlock (to be able to sleep)
  }

  LOG(DEBUG) << "Exiting TfBuilderInfo-Housekeeping thread...";
}

}
} /* o2::DataDistribution */
