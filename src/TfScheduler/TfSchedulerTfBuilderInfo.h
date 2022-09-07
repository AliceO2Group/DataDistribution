// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

#ifndef ALICEO2_TF_SCHEDULER_TFBUILDER_INFO_H_
#define ALICEO2_TF_SCHEDULER_TFBUILDER_INFO_H_

#include <ConfigParameters.h>
#include <ConfigConsul.h>

#include <StfSenderRpcClient.h>

#include <discovery.pb.h>
#include <discovery.grpc.pb.h>
#include <grpcpp/grpcpp.h>

#include <Utilities.h>
#include <DataDistributionOptions.h>

#include <vector>
#include <map>
#include <deque>
#include <thread>
#include <chrono>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

struct TfBuilderInfo {
  std::chrono::steady_clock::time_point mUpdateLocalTime;
  TfBuilderUpdateMessage mTfBuilderUpdate;
  std::uint64_t mLastScheduledTf = 0;
  std::uint64_t mEstimatedFreeMemory = 0;
  std::uint64_t mReportedFreeMemory = 0;
  std::size_t mTfsInBuilding = 0;
  std::size_t mTfsBuffered = 0;

  // Topological distribution
  double mTotalUtilization = 0.0;

  TfBuilderInfo() = delete;

  TfBuilderInfo(std::chrono::steady_clock::time_point pUpdateLocalTime, const TfBuilderUpdateMessage &pTfBuilderUpdate)
  : mUpdateLocalTime(pUpdateLocalTime),
    mTfBuilderUpdate(pTfBuilderUpdate)
  {
    mEstimatedFreeMemory = mTfBuilderUpdate.free_memory();
    mReportedFreeMemory = mTfBuilderUpdate.free_memory();
  }

  const std::string& id() const { return mTfBuilderUpdate.info().process_id(); }
  std::uint64_t last_scheduled_tf_id() const { return mLastScheduledTf; }
  std::uint64_t last_built_tf_id() const { return mTfBuilderUpdate.last_built_tf_id(); }
};

struct TfBuilderTopoInfo {
    std::uint64_t mSubSpec;
    char mDataOrigin[4];

    TfBuilderTopoInfo() = default;
    TfBuilderTopoInfo(const std::string_view pDataOrigin, const std::uint64_t pSubSoec)
    : mSubSpec(pSubSoec)
    {
      std::copy_n(pDataOrigin.cbegin(), 3, mDataOrigin); mDataOrigin[3] = '\0';
    }

    std::size_t operator()(TfBuilderTopoInfo const& s) const noexcept;
};

bool operator==(const TfBuilderTopoInfo& lhs, const TfBuilderTopoInfo& rhs);

class TfSchedulerTfBuilderInfo
{
 public:

  TfSchedulerTfBuilderInfo() = delete;
  TfSchedulerTfBuilderInfo(std::shared_ptr<ConsulTfScheduler> pDiscoveryConfig)
  : mDiscoveryConfig(pDiscoveryConfig)
  {
    mGlobalInfo.reserve(1000); // number of EPNs
  }

  ~TfSchedulerTfBuilderInfo() { }

  void start() {
    mGlobalInfo.clear();

    mRunning = true;
    // start gRPC client monitoring thread
    mHousekeepingThread = create_thread_member("sched_tfb_mon", &TfSchedulerTfBuilderInfo::HousekeepingThread, this);
  }

  void stop() {
    DDDLOG("TfSchedulerTfBuilderInfo::stop()");
    mRunning = false;

    if (mHousekeepingThread.joinable()) {
      mHousekeepingThread.join();
    }

    {
      std::scoped_lock lLock(mGlobalInfoLock, mReadyInfoLock, mTopoInfoLock);
      // delete all info
      mGlobalInfo.clear();
      mReadyTfBuilders.clear();
      mTopoTfBuilders.clear();
    }
  }

  void HousekeepingThread();

  void updateTfBuilderInfo(const TfBuilderUpdateMessage &pTfBuilderUpdate);

  void addReadyTfBuilder(std::shared_ptr<TfBuilderInfo> pInfo)
  {
    std::scoped_lock lLock(mReadyInfoLock);
    if (rand() % 2) {
      mReadyTfBuilders.push_back(std::move(pInfo));
    } else {
      mReadyTfBuilders.push_front(std::move(pInfo));
    }
  }

  void removeReadyTfBuilder(const std::string &pId)
  {
    std::scoped_lock lLock(mReadyInfoLock, mTopoInfoLock);
    for (auto it = mReadyTfBuilders.begin(); it != mReadyTfBuilders.end(); it++) {
      if ((*it)->id() == pId) {
        IDDLOG("Removed TfBuilder from the ready list. tfb_id={}", pId);
        mReadyTfBuilders.erase(it);
        break;
      }
    }

    // remove all assignments
    for (auto it = mTopoTfBuilders.cbegin(); it != mTopoTfBuilders.cend(); /* inc below */ ) {
      if (it->second->id() == pId) {
        IDDLOG("Removed TfBuilder from the topo list. tfb_id={} equip={}/{}", pId, it->first.mDataOrigin, it->first.mSubSpec);
        mTopoTfBuilders.erase(it++);
      } else {
        ++it;
      }
    }
  }

  bool findTfBuilderForTf(const std::uint64_t pSize, std::string& pTfBuilderId /*out*/);

  bool markTfBuilderWithTfId(const std::string& pTfBuilderId, const std::uint64_t pTfIf)
  {
    std::scoped_lock lLock(mGlobalInfoLock, mReadyInfoLock);
    if (mGlobalInfo.count(pTfBuilderId) > 0) {
      auto &lTfBld = mGlobalInfo[pTfBuilderId];
      lTfBld->mLastScheduledTf = pTfIf;
      lTfBld->mTfsInBuilding += 1; // will be updated by TfBuilder updates
      return true;
    }
    return false;
  }


  /// Topological distribution
  bool findTfBuilderForTopoStf(const std::string_view pDataOrigin, const std::uint64_t pSubSpec, std::string& pTfBuilderId /*out*/);

  void setMaxTfsInBuilding(const std::size_t pMaxTfsInBuilding) {
    const auto lNewVal = std::clamp(pMaxTfsInBuilding, std::size_t(1), std::size_t(512));

    if (mMaxTfsInBuilding != lNewVal) {
      IDDLOG("Parameter Update: MaxTfsInBuilding old_value={} new_value={}", mMaxTfsInBuilding, lNewVal);
      mMaxTfsInBuilding = lNewVal;
    }
  }

  void resetCounters() {
    mNoTfBuilderAvailable = 0;
    mNoMemoryAvailable = 0;
    mTfNumExceeeded = 0;
  }

private:
  /// Overestimation of actual size for TF building
  static constexpr std::uint64_t sTfSizeOverestimatePercent = std::uint64_t(20);

  /// Discard timeout for non-complete TFs
  static constexpr auto sTfBuilderDiscardTimeout = 5s;

  /// Discovery configuration
  std::shared_ptr<ConsulTfScheduler> mDiscoveryConfig;

  /// Housekeeping thread
  std::atomic_bool mRunning = false;
  std::thread mHousekeepingThread;

  // Maximum number of TFs each TfBuilder is allowed to aggregate concurrently
  std::atomic_uint64_t mMaxTfsInBuilding = 16;

  /// TfSender global info
  mutable std::recursive_mutex mGlobalInfoLock;
    std::unordered_map<std::string, std::shared_ptr<TfBuilderInfo>> mGlobalInfo;

  /// List of TfBuilders with available resources
  mutable std::recursive_mutex mReadyInfoLock;
    std::deque<std::shared_ptr<TfBuilderInfo>> mReadyTfBuilders;

  /// List of TfBuilders for Topological distribution
  mutable std::recursive_mutex mTopoInfoLock;
    std::unordered_map<TfBuilderTopoInfo, std::shared_ptr<TfBuilderInfo>, TfBuilderTopoInfo> mTopoTfBuilders;


  /// Counters
  std::atomic_uint64_t mNoTfBuilderAvailable = 0;
  std::atomic_uint64_t mNoMemoryAvailable = 0;
  std::atomic_uint64_t mTfNumExceeeded = 0;
};


} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_SCHEDULER_TFBUILDER_INFO_H_ */
