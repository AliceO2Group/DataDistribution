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

#include <DataDistMonitoring.h>

#include <set>
#include <tuple>
#include <algorithm>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

std::size_t TfBuilderTopoInfo::operator()(TfBuilderTopoInfo const& s) const noexcept
{
    std::size_t h1 = std::hash<std::uint64_t>{}(mSubSpec);
    std::size_t h2 = std::hash<std::string_view>{}(std::string_view(s.mDataOrigin, 3));
    return h1 ^ (h2 << 1);
}

bool operator==(const TfBuilderTopoInfo& lhs, const TfBuilderTopoInfo& rhs)
{
    return lhs.mSubSpec == rhs.mSubSpec && (0 == std::memcmp(lhs.mDataOrigin, rhs.mDataOrigin, 3));
}

void TfSchedulerTfBuilderInfo::updateTfBuilderInfo(const TfBuilderUpdateMessage &pTfBuilderUpdate)
{
  using namespace std::chrono_literals;
  const auto lLocalTime = std::chrono::steady_clock::now();

  // recreate timepoint from the received millisecond time stamp
  const std::chrono::milliseconds lUpdateDuration(pTfBuilderUpdate.info().last_update_t());
  const std::chrono::time_point<std::chrono::system_clock> lUpdateTimepoint(lUpdateDuration);
  const auto &lTfBuilderId = pTfBuilderUpdate.info().process_id();

  // check for system time drifts; account for gRPC latency
  const auto lTimeDiff = std::chrono::system_clock::now() - lUpdateTimepoint;
  if (std::chrono::abs(lTimeDiff) > 1s) {
    WDDLOG("Large system clock drift detected. tfb_id={} drift_ms={}", lTfBuilderId,
      std::chrono::duration_cast<std::chrono::milliseconds>(lTimeDiff).count());
  }

  {
    // lock the global info
    std::scoped_lock lLock(mGlobalInfoLock);

    // check if should remove
    if (BasicInfo::NOT_RUNNING == pTfBuilderUpdate.info().process_state()) {

      const auto &lTfIter = mGlobalInfo.find(lTfBuilderId);
      if (lTfIter != mGlobalInfo.end()) {
        // remove from available
        removeReadyTfBuilder(lTfBuilderId);
        // remove from global
        mGlobalInfo.erase(lTfIter);
        IDDLOG("TfBuilder left the partition. tfb_id={} reason=NOT_RUNNING", lTfBuilderId);
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

      IDDLOG("TfBuilder joined the partition. tfb_id={}", lTfBuilderId);
    } else {
      auto &lInfo = mGlobalInfo.at(lTfBuilderId);

      // acquire the ready lock, since the data is shared
      std::scoped_lock lLockReady(mReadyInfoLock);
      lInfo->mUpdateLocalTime = lLocalTime;
      lInfo->mReportedFreeMemory = pTfBuilderUpdate.free_memory();
      lInfo->mTfsInBuilding = pTfBuilderUpdate.num_tfs_in_building();
      lInfo->mTfsBuffered = pTfBuilderUpdate.num_buffered_tfs();

      // update only when the last scheduled tf is built!
      if (pTfBuilderUpdate.last_built_tf_id() == lInfo->last_scheduled_tf_id()) {
        // store the new information
        lInfo->mTfBuilderUpdate = pTfBuilderUpdate;

        // verify the memory estimation is correct
        if (lInfo->mEstimatedFreeMemory > pTfBuilderUpdate.free_memory()) {
          DDDLOG("TfBuilder memory estimate is too high. tfb_id={} mem_estimate={}", lTfBuilderId,
            (double(lInfo->mEstimatedFreeMemory) / double(pTfBuilderUpdate.free_memory())));
        }

        lInfo->mEstimatedFreeMemory = pTfBuilderUpdate.free_memory();

      } else if (pTfBuilderUpdate.last_built_tf_id() < lInfo->last_scheduled_tf_id()) {

        // update scheduler's estimate to be on the safe side
        if (lInfo->mEstimatedFreeMemory > pTfBuilderUpdate.free_memory() ) {

          DDDLOG("Ignoring TfBuilder info (last_build < last_scheduled). Fixing the estimate ratio. "
            "tfb_id={} new_mem_estimate={}", lTfBuilderId, pTfBuilderUpdate.free_memory());

          lInfo->mEstimatedFreeMemory = pTfBuilderUpdate.free_memory();

        } else {
          // if (last_build > last_scheduled)
          // NOTE: there is a "race" between notifying the EPN to build and updating last_scheduled_tf_id
          // in our record. Thus, this codepath is possible, and we should update the est memory since we
          // hold the lock
          lInfo->mEstimatedFreeMemory = std::min(lInfo->mEstimatedFreeMemory, pTfBuilderUpdate.free_memory());
        }
      }
    }
  } // mGlobalInfoLock unlock
}

bool TfSchedulerTfBuilderInfo::findTfBuilderForTf(const std::uint64_t pSize, std::string& pTfBuilderId /*out*/)
{
  // NOTE: we will overestimate memory requirement by a factor, until TfBuilder updates
  //       us with the actual size.
  const std::uint64_t lTfEstSize = pSize * (sTfSizeOverestimatePercent + 100) / 100;

  std::scoped_lock lLock(mReadyInfoLock);

  uint64_t lMaxMem = 0;
  bool lMaxTfExceeded = false;
  bool lNoMemory = false;
  auto lIt = mReadyTfBuilders.begin();
  for (; lIt != mReadyTfBuilders.end(); ++lIt) {
    lMaxMem = std::max(lMaxMem, (*lIt)->mEstimatedFreeMemory);

    // check number of time frames in building
    if ((*lIt)->mTfsInBuilding >= mMaxTfsInBuilding) {
      lMaxTfExceeded = true;
      continue;
    }

    // check if enough memory available
    if ((*lIt)->mEstimatedFreeMemory < lTfEstSize) {
      lNoMemory = true;
      continue;
    }

    // sometimes TfBuilder buffers too much TFs, eg. when processing fails, and fragmentation of tf buffer can
    // prevent new TF allocation, stalling transferrs
    if (((*lIt)->mTfsBuffered > 8) && ((*lIt)->mEstimatedFreeMemory < (4 * lTfEstSize) )) {
      lNoMemory = true;
      continue;
    }

    // we found the suitable TfBuilder
    break;
  }

  // TfBuilder not found?
  if ( lIt == mReadyTfBuilders.end() ) {
    if (mReadyTfBuilders.empty()) {
      ++mNoTfBuilderAvailable;
      DDMON("tfscheduler", "tf.rejected.no_tfb_inst", mNoTfBuilderAvailable);

      WDDLOG_RL(10000, "FindTfBuilder: TF cannot be scheduled. reason=NO_TFBUILDERS total={}",
        mNoTfBuilderAvailable);

    } else if (lMaxTfExceeded) {
      ++mTfNumExceeeded;
      WDDLOG_RL(10000, "FindTfBuilder: TF cannot be scheduled. reason=NUM_TF_EXCEEEDED total={} tf_size={} ready_tfb={}",
        mTfNumExceeeded, lTfEstSize, mReadyTfBuilders.size());
      DDMON("tfscheduler", "tf.rejected.max_tf_exceeded", mTfNumExceeeded);
    } else if (lNoMemory) {
      ++mNoMemoryAvailable;
      DDMON("tfscheduler", "tf.rejected.no_tfb_buf", mNoMemoryAvailable);
      WDDLOG_RL(10000, "FindTfBuilder: TF cannot be scheduled. reason=NO_MEMORY total={} tf_size={} ready_tfb={}",
        mNoMemoryAvailable, lTfEstSize, mReadyTfBuilders.size());
    }
    return false;
  }

  // reposition the selected StfBuilder to the end of the list
  auto lTfBuilder = std::move(*lIt);

  assert (lTfBuilder->mEstimatedFreeMemory >= lTfEstSize);

  // copy the string out
  assert (!lTfBuilder->id().empty());
  pTfBuilderId = lTfBuilder->id();

  // deque erase reverse_iterator
  mReadyTfBuilders.erase(lIt);

  lTfBuilder->mEstimatedFreeMemory -= lTfEstSize;
  mReadyTfBuilders.push_back(std::move(lTfBuilder));

  return true;
}


bool TfSchedulerTfBuilderInfo::findTfBuilderForTopoStf(const std::string_view pDataOrigin, const std::uint64_t pSubSpec, std::string& pTfBuilderId /*out*/)
{
  // look for a candidate in the assigned list
  std::scoped_lock lLock(mReadyInfoLock, mTopoInfoLock);
  {
    const auto &lTfBuilderInfo = mTopoTfBuilders.find({pDataOrigin, pSubSpec});
    if (lTfBuilderInfo != mTopoTfBuilders.end()) {
      pTfBuilderId = lTfBuilderInfo->second->id();
      return true;
    }
  }

  // assign the least utilized EPN from the ready list
  {
    double lMinUtil = std::numeric_limits<double>::max();
    std::shared_ptr<TfBuilderInfo> lMinTfBuilder;

    for (auto &lIt : mReadyTfBuilders) {
      if (lIt->mTotalUtilization < lMinUtil) {
        lMinUtil = lIt->mTotalUtilization;
        lMinTfBuilder = lIt;
      }
    }

    if (!lMinTfBuilder) {
      WDDLOG_RL(1000, "FindTfBuilderTopo: TF cannot be scheduled. reason=NO_TFBUILDERS total={}", mReadyTfBuilders.size());
      return false;
    }

    pTfBuilderId = lMinTfBuilder->id();
    lMinTfBuilder->mTotalUtilization += 1.0; // TODO: Fix weights and network groups?

    // add utilization and cache the TfBuilder
    mTopoTfBuilders.emplace(std::piecewise_construct,
      std::forward_as_tuple(pDataOrigin, pSubSpec),
      std::forward_as_tuple(lMinTfBuilder)
    );

    DDDLOG("Topological TfBuilder Assignment: {}/{} -> {}", pDataOrigin, pSubSpec, pTfBuilderId);

    return true;
  }

  return false;
}

void TfSchedulerTfBuilderInfo::HousekeepingThread()
{
  using namespace std::chrono_literals;

  DDDLOG("Starting TfBuilderInfo-Housekeeping thread.");

  std::vector<std::string> lIdsToErase;

  while (mRunning) {
    // update scheduling parameters
    setMaxTfsInBuilding(mDiscoveryConfig->getUInt64Param(MaxNumTfsInBuildingKey, MaxNumTfsInBuildingDevault));

    {
      std::scoped_lock lLock(mGlobalInfoLock);

      // reap stale TfBuilders
      assert (lIdsToErase.empty());
      for (const auto &lIdInfo : mGlobalInfo) {
        const auto &lInfo = lIdInfo.second;
        const auto lNow = std::chrono::steady_clock::now();
        const auto lTimeDiff = std::chrono::abs(lNow - lInfo->mUpdateLocalTime);
        if (lTimeDiff >= sTfBuilderDiscardTimeout) {
          lIdsToErase.push_back(lInfo->mTfBuilderUpdate.info().process_id());
        }

        DDDLOG("TfBuilder information: tfb_id={} free_memory_est={} free_memory_rep={} num_buffered_tfs={} num_tf_in_building={}",
          lInfo->mTfBuilderUpdate.info().process_id(), lInfo->mEstimatedFreeMemory, lInfo->mReportedFreeMemory,
          lInfo->mTfBuilderUpdate.num_buffered_tfs(), lInfo->mTfsInBuilding);
      }
    } // mGlobalInfoLock unlock (to be able to sleep)

    if (!lIdsToErase.empty()) {
      for (const auto &lId : lIdsToErase) {
        std::scoped_lock lLock(mGlobalInfoLock);

        mGlobalInfo.erase(lId);
        removeReadyTfBuilder(lId);
        WDDLOG("TfBuilder removed from the partition. reason=STALE_INFO tfb_id={}", lId);
      }
      lIdsToErase.clear();
    }

    std::this_thread::sleep_for(2000ms);
  }

  DDDLOG("Exiting TfBuilderInfo-Housekeeping thread.");
}

} /* o2::DataDistribution */
