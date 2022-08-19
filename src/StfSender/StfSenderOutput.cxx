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

#include "StfSenderOutput.h"
#include "StfSenderDevice.h"

#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameVisitors.h>

#include <DataDistributionOptions.h>

#include <fairmq/tools/Unique.h>

#include <algorithm>
#include <condition_variable>
#include <stdexcept>
#include <random>

#if defined(__linux__)
#include <unistd.h>
#endif

namespace o2::DataDistribution
{

using namespace std::chrono_literals;
using timepoint = std::chrono::steady_clock::time_point;

void StfSenderOutput::start(std::shared_ptr<ConsulStfSender> pDiscoveryConfig)
{
  assert(pDiscoveryConfig);
  mDiscoveryConfig = pDiscoveryConfig;
  mRunning = true;

  // Get DD buffer size option
  auto lBufferSizeMB = mDiscoveryConfig->getUInt64Param(StfBufferSizeMBKey, StfBufferSizeMBDefault);
  auto lOptBufferSize = lBufferSizeMB << 20;
  if (lOptBufferSize != mBufferSize) {
    lOptBufferSize = std::clamp(lOptBufferSize, std::uint64_t(8ULL << 30), std::uint64_t(64ULL << 30));
    mBufferSize = lOptBufferSize;
    WDDLOG("StfSender buffer size override. size={}", lOptBufferSize);
  }

  auto lTransportOpt = pDiscoveryConfig->getStringParam(DataDistNetworkTransportKey, DataDistNetworkTransportDefault);

  if (lTransportOpt == "fmq" || lTransportOpt == "FMQ" || lTransportOpt == "fairmq" || lTransportOpt == "FAIRMQ") {
    // create a socket and connect
    mDevice.GetConfig()->SetProperty<int>("io-threads", (int) std::min(std::thread::hardware_concurrency(), 20u));
    mZMQTransportFactory = fair::mq::TransportFactory::CreateTransportFactory("zeromq", "", mDevice.GetConfig());

    // create FairMQ output
    mOutputFairMQ = std::make_unique<StfSenderOutputFairMQ>(pDiscoveryConfig, mCounters);
    mOutputFairMQ->start(mZMQTransportFactory);
  } else {
    // create UCX output
    mOutputUCX = std::make_unique<StfSenderOutputUCX>(pDiscoveryConfig, mCounters);
    mOutputUCX->start();
  }

  // create stf ordering thread
  mStfOrderThread = create_thread_member("stfs_order", &StfSenderOutput::StfOrderThread, this);

  // create stf copy thread
  for (auto i = 0; i < 8; i++) {
    mCopyThreads.emplace_back(create_thread_member("stfs_copy", &StfSenderOutput::StfCopyThread, this));
  }

  // create stf drop thread
  mStfDropThread = create_thread_member("stfs_drop", &StfSenderOutput::StfDropThread, this);

  // create stale stf thread
  mStaleStfThread = create_thread_member("stfs_stale", &StfSenderOutput::StaleStfThread, this);

  // create scheduler thread
  mSchedulerThread = create_thread_member("stfs_sched", &StfSenderOutput::StfSchedulerThread, this);

  // create monitoring thread
  mMonitoringThread = create_thread_member("stfs_mon", &StfSenderOutput::StfMonitoringThread, this);
}

void StfSenderOutput::register_regions()
{
  // this must be called from InitTask()
  // register for region updates
  mDevice.Transport()->SubscribeToRegionEvents([this](fair::mq::RegionInfo info) {
    if (fair::mq::RegionEvent::created == info.event) {
      DDDLOG("Region created. size={} managed={}", info.size, info.managed);
      if (mOutputUCX) {
        mOutputUCX->registerSHMRegion(info.ptr, info.size, info.managed, info.flags);
      }
    } else if (fair::mq::RegionEvent::destroyed == info.event) {
      DDDLOG("Region destroyed while running. size={} managed={}", info.size, info.managed);
    }
  });
}

void StfSenderOutput::start_standalone(std::shared_ptr<ConsulStfSender> pDiscoveryConfig)
{
  assert(pDiscoveryConfig);
  mDiscoveryConfig = pDiscoveryConfig;
  mRunning = true;

  // Get DD buffer size option
  auto lBufferSizeMB = mDiscoveryConfig->getUInt64Param(StfBufferSizeMBKey, StfBufferSizeMBDefault);
  auto lOptBufferSize = lBufferSizeMB << 20;
  if (lOptBufferSize != mBufferSize) {
    lOptBufferSize = std::clamp(lOptBufferSize, std::uint64_t(8ULL << 30), std::uint64_t(64ULL << 30));
    mBufferSize = lOptBufferSize;
    WDDLOG("StfSender buffer size override. size={}", lOptBufferSize);
  }
  // Get the keep target
  mKeepTarget = mDiscoveryConfig->getUInt64Param(StandaloneStfDataBufferSizeMBKey, StandaloneStfDataBufferSizeMBDefault) << 20;
  mKeepTarget = std::clamp(mKeepTarget.load(), std::uint64_t(128ULL << 20), mBufferSize);

  // create stf ordering thread
  mStfOrderThread = create_thread_member("stfs_order", &StfSenderOutput::StfOrderThread, this);

  // create stf copy thread
  for (auto i = 0; i < 8; i++) {
    mCopyThreads.emplace_back(create_thread_member("stfs_copy", &StfSenderOutput::StfCopyThread, this));
  }

  // create stf keep thread
  mStfKeepThread = create_thread_member("stfs_keep", &StfSenderOutput::StfKeepThread, this);
  // create stf drop thread
  mStfDropThread = create_thread_member("stfs_drop", &StfSenderOutput::StfDropThread, this);
  // create monitoring thread
  mMonitoringThread = create_thread_member("stfs_mon", &StfSenderOutput::StfMonitoringThread, this);
}

void StfSenderOutput::stop()
{
  mRunning = false;
  mDropQueue.stop();
  mScheduleQueue.stop();
  mCopyQueue.stop();

  // stop the stf ordering: on pipeline interrupt
  if (mStfOrderThread.joinable()) {
    mStfOrderThread.join();
  }

  // stop copy threads
  for (auto &lThread : mCopyThreads) {
    if (lThread.joinable()) {
      lThread.join();
    }
  }
  if (mStfCopyBuilder) {
    mStfCopyBuilder->stop();
    mStfCopyBuilder.reset();
  }

  // stop the scheduler
  if (mSchedulerThread.joinable()) {
    mSchedulerThread.join();
  }

  // Stop standalone KeepThread
  if (mStfKeepThread.joinable()) {
    mStfKeepThread.join();
  }

  // stop the stale thread
  if (mStaleStfThread.joinable()) {
    mStaleStfThread.join();
  }

  // stop the drop queue
  if (mStfDropThread.joinable()) {
    mStfDropThread.join();
  }

  // stop FaiMQ input
  if (mOutputFairMQ) {
    mOutputFairMQ->stop();
  }

  // Unsubscribe from region updates
  mDevice.Transport()->UnsubscribeFromRegionEvents();

  // stop UCX input
  if (mOutputUCX) {
    mOutputUCX->stop();
  }

  // wait for monitoring thread
  if (mMonitoringThread.joinable()) {
    mMonitoringThread.join();
  }
}

bool StfSenderOutput::running() const
{
  return mDevice.IsRunningState();
}

ConnectStatus StfSenderOutput::connectTfBuilder(const std::string &pTfBuilderId, const std::string &pEndpoint)
{
  if (!mOutputFairMQ) {
    return ConnectStatus::eCONNERR;
  }
  return mOutputFairMQ->connectTfBuilder(pTfBuilderId, pEndpoint);
}

bool StfSenderOutput::disconnectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint)
{
  if (!mOutputFairMQ) {
    return false;
  }
  return mOutputFairMQ->disconnectTfBuilder(pTfBuilderId, lEndpoint);
}

ConnectStatus StfSenderOutput::connectTfBuilderUCX(const std::string &pTfBuilderId, const std::string &pIp, unsigned pPort)
{
  if (!mOutputUCX) {
    return ConnectStatus::eCONNERR;
  }
  return mOutputUCX->connectTfBuilder(pTfBuilderId, pIp, pPort);
}
bool StfSenderOutput::disconnectTfBuilderUCX(const std::string &pTfBuilderId)
{
  if (!mOutputUCX) {
    return false;
  }
  return mOutputUCX->disconnectTfBuilder(pTfBuilderId);
}

// Keep configurable number of STFs until the set goal is reached
// Only used in standalone runs
void StfSenderOutput::StfKeepThread()
{
  std::default_random_engine lGen;
  std::uniform_int_distribution<unsigned> lUniformDist(0, 99);

  mDeletePercentage = mDiscoveryConfig->getUInt64Param(StandaloneStfDeleteChanceKey, StandaloneStfDeleteChanceDefault);
  mDeletePercentage = std::clamp(mDeletePercentage.load(), std::uint64_t(0), std::uint64_t(100));

  while (mRunning) {
    auto lStfOpt = mScheduleQueue.pop_wait_for(50ms);

    std::scoped_lock lMapLock(mStfKeepMapLock);

    // get buffer sizes
    StdSenderOutputCounters::Values lCounters;
    {
      std::scoped_lock lLock(mCounters.mCountersLock);
      lCounters = mCounters.mValues;
    }

    // Maintain the setpoint
    while (!mStfKeepMap.empty() && (lCounters.mBuffered.mSize > (mKeepTarget * 95 / 100))) {
      // over the limit, delete:
      auto lStfIdToRemove = mStfKeepMap.rbegin()->first;
      lCounters.mBuffered.mSize -= mStfKeepMap[lStfIdToRemove]->getDataSize();
      mDropQueue.push(std::move(mStfKeepMap[lStfIdToRemove]));
      mStfKeepMap.erase(lStfIdToRemove);
    }

    // Random chance to delete the oldest stf
    if (!mStfKeepMap.empty() && (lUniformDist(lGen) <= mDeletePercentage)) {
      auto lStfIdToRemove = mStfKeepMap.rbegin()->first;
      mDropQueue.push(std::move(mStfKeepMap[lStfIdToRemove]));
      mStfKeepMap.erase(lStfIdToRemove);
    }

    // Delete out of order
    if (!mStfKeepMap.empty() && (lUniformDist(lGen) <= (mDeletePercentage / 4))) {
      const auto lSize = mStfKeepMap.size();
      const auto lIdx = lSize * lUniformDist(lGen) / 200; // prioritize small index

      auto lIter = mStfKeepMap.begin();
      std::advance(lIter, lIdx);

      mDropQueue.push(std::move(lIter->second));
      mStfKeepMap.erase(lIter->first);
    }

    if (lStfOpt) {
      std::unique_ptr<SubTimeFrame> lStf = std::move(lStfOpt.value());

      const auto lStfId = lStf->id();
      const auto lStfSize = lStf->getDataSize();

      if (mStfKeepMap.count(lStfId) == 0) {
        mStfKeepMap[lStfId] = std::move(lStf);
        // update buffer sizes
        {
          std::scoped_lock lLock(mCounters.mCountersLock);
          mCounters.mValues.mBuffered.mSize += lStfSize;
          mCounters.mValues.mBuffered.mCnt += 1;
        }
      }
    }
  }

  std::scoped_lock lMapLock(mStfKeepMapLock);
  mStfKeepMap.clear();
  DDDLOG("StfKeepThread: Exiting.");
}

void StfSenderOutput::StfOrderThread()
{
  std::unique_ptr<SubTimeFrame> lStf;
  while ((lStf = mPipelineI.dequeue(eSenderIn)) != nullptr) {

    DDDLOG_RL(1000, "StfOrderThread: receiving {}", lStf->id() );

    std::unique_lock lOrderLock(mScheduledStfMapLock);
    mStfOrderingQueue.push(lStf->id());
    // push the original Stf for scheduling
    mCopyQueue.push(std::move(lStf));
  }
  DDDLOG("StfOrderThread: Exiting.");
}

void StfSenderOutput::StfCopyThread()
{
  while (mRunning) {
    std::unique_ptr<SubTimeFrame> lStf;
    if (!mCopyQueue.pop(lStf)) {
      break;
    }

    if (mStfCopyBuilder) {
      // copy stf
      if (!mStfCopyBuilder->copyStfData(lStf)) {
        DDMON("stfsender", "stf_region.full", 1);
      }
    }

    // wait for our turn to schedule
    while (mRunning) {
      std::unique_lock lOrderLock(mScheduledStfMapLock);
      assert (!mStfOrderingQueue.empty());
      assert (mStfOrderingQueue.front() <= lStf->id());

      if (mStfOrderingQueue.front() != lStf->id()) {
        mStfOrderingCv.wait_for(lOrderLock, 10ms);
        continue;
      } else {
        mStfOrderingQueue.pop();
        // push the original Stf for scheduling
        mScheduleQueue.push(std::move(lStf));
        break;
      }
    }
    mStfOrderingCv.notify_all();
  }

  DDDLOG("StfCopyThread: Exiting.");
}

// Reports incoming STFs to TfScheduler and queues them for transmitting or dropping
void StfSenderOutput::StfSchedulerThread()
{
  // Notifies the scheduler about stfs
  DDDLOG("StfSchedulerThread: Starting.");

  // increase the priority
#if defined(__linux__)
  if (nice(-10)) {}
#endif

  while (mRunning) {
    std::unique_ptr<SubTimeFrame> lStf;
    if (!mScheduleQueue.pop(lStf)) {
      break;
    }

    const auto lStfId = lStf->id();
    const auto lStfSize = lStf->getDataSize();

    // update buffer sizes
    StdSenderOutputCounters::Values lCounters;
    {
      std::scoped_lock lLock(mCounters.mCountersLock);
      // check for missing Stfs
      if (lStfId > mLastStfId) {
        mCounters.mValues.mTotalSent.mMissing += (lStfId - mLastStfId - 1);
      } else {
        EDDLOG_RL(5000, "StfSender received an SubTimeFrame out of order. received_stf_id={} expected_stf_id={}",
          lStfId, (mLastStfId + 1));
      }
      mLastStfId = std::max(mLastStfId, lStfId);

      mCounters.mValues.mBuffered.mSize += lStfSize;
      mCounters.mValues.mBuffered.mCnt += 1;

      lCounters = mCounters.mValues;
    }

    assert (!mDevice.standalone());

    if (!mDevice.TfSchedRpcCli().is_ready()) {
      IDDLOG_RL(10000, "StfSchedulerThread: TfScheduler gRPC connection is not ready stf_id={}", lStfId);
      mDropQueue.push(std::move(lStf));
      continue;
    }

    DDDLOG_RL(5000, "StfSchedulerThread: Scheduling stf_id={}", lStfId);

    StfSenderStfInfo lStfInfo;
    SchedulerStfInfoResponse lSchedResponse;

    const auto &lStatus = mDiscoveryConfig->status();
    lStfInfo.mutable_info()->CopyFrom(lStatus.info());
    lStfInfo.mutable_partition()->CopyFrom(lStatus.partition());
    lStfInfo.set_stf_id(lStfId);
    lStfInfo.set_stf_size(lStfSize);

    lStfInfo.mutable_stfs_info()->set_buffer_size(mBufferSize);
    lStfInfo.mutable_stfs_info()->set_buffer_used(lCounters.mBuffered.mSize);
    lStfInfo.mutable_stfs_info()->set_num_buffered_stfs(lCounters.mBuffered.mCnt);

    switch (lStf->header().mOrigin) {
      case SubTimeFrame::Header::Origin::eReadout:
      {
        lStfInfo.set_stf_source(StfSource::DEFAULT);
        break;
      }
      case SubTimeFrame::Header::Origin::eReadoutTopology:
      {
        lStfInfo.set_stf_source(StfSource::TOPOLOGICAL);
        auto lInfoPtr = lStfInfo.mutable_stf_source_info()->Add();

        const auto &lStfEquip = lStf->getEquipmentIdentifiers();
        if (lStfEquip.size() != 1) {
          EDDLOG_RL(1000, "StfSchedulerThread: number of equipments is not 1. num_equipments={}", lStfEquip.size());
          mDropQueue.push(std::move(lStf));
          continue;
        }

        lInfoPtr->set_data_origin(lStfEquip.begin()->mDataOrigin.as<std::string>());
        lInfoPtr->set_data_subspec(lStfEquip.begin()->mSubSpecification);
        break;
      }
      default:
      {
        EDDLOG_RL(1000, "StfSchedulerThread: dropping STF of unknown type stf_source={}", lStf->header().mOrigin);
        mDropQueue.push(std::move(lStf));
        continue;
        break;
      }
    }

    // to avoid races, move the stf into the triage map before notifying the scheduler
    {
      std::scoped_lock lLock(mScheduledStfMapLock);

      if (mScheduledStfMap.count(lStfId) > 0) {
        EDDLOG_RL(500, "StfSchedulerThread: Stf is already scheduled! Skipping the duplicate. stf_id={}", lStfId);
        mDropQueue.push(std::move(lStf));
        continue;
      }

      mScheduledStfMap.emplace(lStfId, ScheduledStfInfo{ std::move(lStf), timepoint::clock::now(), timepoint{} } );
    }

    // Send STF info to scheduler
    {
      DDDLOG_RL(5000, "StfSchedulerThread: Sending an STF announce... stf_id={} stf_size={}", lStfId, lStfInfo.stf_size());
      const auto lSentOK = mDevice.TfSchedRpcCli().StfSenderStfUpdate(lStfInfo, lSchedResponse);
      // check if the scheduler rejected the data
      if (!lSentOK || (lSchedResponse.status() != SchedulerStfInfoResponse::OK)) {
        { // drop the stf
          std::scoped_lock lLock(mScheduledStfMapLock);
          // find the stf in the map and erase it
          const auto lStfIter = mScheduledStfMap.find(lStfId);
          if (lStfIter != mScheduledStfMap.end()) {
            mDropQueue.push(std::move(lStfIter->second.mStf));
            mScheduledStfMap.erase(lStfIter);
          }
        }
        { // scheduler rejected metric
          std::scoped_lock lCntLock(mCounters.mCountersLock);
          mCounters.mValues.mSchedulerStfRejectedCnt += 1;
        }

        WDDLOG_RL(5000, "TfScheduler rejected the Stf announce. stf_id={} reason={}",
          lStfId, SchedulerStfInfoResponse_StfInfoStatus_Name(lSchedResponse.status()));
      }
      DDDLOG_RL(5000, "StfSchedulerThread: Sent an STF announce. stf_id={} stf_size={}", lStfId, lStfInfo.stf_size());
    }
  }

  DDDLOG("StfSchedulerThread: Exiting.");
}

void StfSenderOutput::sendStfToTfBuilder(const std::uint64_t pStfId, const std::string &pTfBuilderId, StfDataResponse &pRes)
{
  assert(!pTfBuilderId.empty());
  std::scoped_lock lLock(mScheduledStfMapLock);

  { // Check if we handled this grpc call before, and we are handling a retry now
    const auto lResIter = mSchedulingResult.find(pTfBuilderId);
    if (lResIter != mSchedulingResult.end()) {

      if (lResIter->second.first == pStfId) {
        pRes.set_status(lResIter->second.second);
        return;
      }
    }
  }

  const auto lStfIter = mScheduledStfMap.find(pStfId);
  // verify we have the STF.
  if (lStfIter == mScheduledStfMap.end()) {
    if (pTfBuilderId != "-1") {
      // request for Stf we don't have is an error
      pRes.set_status(StfDataResponse::DATA_DROPPED_UNKNOWN);
      EDDLOG_GRL(1000, "sendStfToTfBuilder: TfBuilder requested non-existing STF. stf_id={}", pStfId);
    } else {
      // we can get a drop request for a STF we didn't have
      pRes.set_status(StfDataResponse::DATA_DROPPED_SCHEDULER);
    }
  } else if (pTfBuilderId == "-1") { // check if it is drop request from the scheduler
    pRes.set_status(StfDataResponse::DATA_DROPPED_SCHEDULER);
    mDropQueue.push(std::move(lStfIter->second.mStf));
    mScheduledStfMap.erase(lStfIter);
    { // scheduler rejected metric
      std::scoped_lock lCntLock(mCounters.mCountersLock);
      mCounters.mValues.mSchedulerStfRejectedCnt += 1;
    }
  } else {

    // extract the StfInfo
    auto lStfInfo = std::move(lStfIter->second);
    auto &lStf = lStfInfo.mStf;
    const auto lStfSize = lStf->getDataSize();
    lStfInfo.mTimeRequested = timepoint::clock::now();
    mScheduledStfMap.erase(lStfIter);

    // send to output backend
    bool lOk = false;
    if (mOutputUCX) {
      lOk = mOutputUCX->sendStfToTfBuilder(pTfBuilderId, std::move(lStfInfo));
    } else if (mOutputFairMQ) {
      lOk = mOutputFairMQ->sendStfToTfBuilder(pTfBuilderId, std::move(lStf));
    }
    if (lOk) {
      // update status and counters
      pRes.set_status(StfDataResponse::OK);
      std::scoped_lock lCntLock(mCounters.mCountersLock);
      mCounters.mValues.mInSending.mSize += lStfSize;
      mCounters.mValues.mInSending.mCnt += 1;
    } else {
      pRes.set_status(StfDataResponse::TF_BUILDER_UNKNOWN);
      mDropQueue.push(std::move(lStf));
      EDDLOG_GRL(1000, "sendStfToTfBuilder: TfBuilder not known to StfSender. tfb_id={}", pTfBuilderId);
    }
  }

  // remember the result for the stf
  auto &lResultInfo = mSchedulingResult[pTfBuilderId];
  lResultInfo.first = pStfId;
  lResultInfo.second = pRes.status();
}

/// Drop thread
void StfSenderOutput::StfDropThread()
{
  DDDLOG("Starting DataDropThread thread.");
  std::uint64_t lNumDroppedStfs = 0;

  while (mRunning) {
    std::unique_ptr<SubTimeFrame> lStf;
    if (!mDropQueue.pop(lStf)) {
      break;
    }

    const auto lStfSize = lStf->getDataSize();

    DDDLOG_GRL(10000, "Dropping an STF. stf_id={} stf_size={} total_dropped_stf={}", lStf->header().mId,
      lStfSize, lNumDroppedStfs);

    // delete the data
    lStf.reset();
    // update buffer status
    lNumDroppedStfs += 1;
    {
      std::scoped_lock lLock(mCounters.mCountersLock);
      mCounters.mValues.mBuffered.mSize -= lStfSize;
      mCounters.mValues.mBuffered.mCnt -= 1;
    }
  }

  DDDLOG("Exiting DataDropThread thread");
}

// Monitor for stale STFs that were not requested by TfBuilders
void StfSenderOutput::StaleStfThread()
{
  DDDLOG("Starting StfStaleStfThread thread.");

  timepoint lLastRun = timepoint::clock::now();

  while (mRunning) {
    while (since<std::chrono::milliseconds>(lLastRun) < 1000.0) { // test stfs every 1 second
      std::this_thread::sleep_for(500ms);
      continue;
    }

    { // Update the parameter from consul
      const auto lStaleStfTimeoutMs = std::clamp(mDiscoveryConfig->getUInt64Param(StaleStfTimeoutMsKey, StaleStfTimeoutMsDefault),
        std::uint64_t(5000), std::uint64_t(300000));

      if (mStaleStfTimeoutMs != lStaleStfTimeoutMs) {
        IDDLOG("StaleStfTimeoutMs new={} old={}", lStaleStfTimeoutMs, mStaleStfTimeoutMs);
        mStaleStfTimeoutMs = lStaleStfTimeoutMs;
      }
    }

    lLastRun = timepoint::clock::now();

    std::scoped_lock lLock(mScheduledStfMapLock);

    while (!mScheduledStfMap.empty()) {
      // check the STF with the lowest id (the oldest)
      auto &lStfInfo = mScheduledStfMap.begin()->second;

      const auto lStfTimeMs = since<std::chrono::milliseconds>(lStfInfo.mTimeAdded);
      if (lStfTimeMs <= mStaleStfTimeoutMs) {
        // nothing to clean
        DDDLOG_RL(10000, "StfStaleStfThread: No STFs to clean. num_stfs={} oldest_ms={}", mScheduledStfMap.size(), lStfTimeMs);
        break;
      }

      IDDLOG_RL(10000, "StfStaleStfThread: Deleting an stale STF. stf_id={} stale_ms={}", lStfInfo.mStf->id(),lStfTimeMs);

      mDropQueue.push(std::move(lStfInfo.mStf));
      mScheduledStfMap.erase(mScheduledStfMap.cbegin());
    }
  }

  DDDLOG("Exiting StfStaleStfThread thread");
}

void StfSenderOutput::StfMonitoringThread()
{
  DDDLOG("Starting StfMonitoring thread.");

  while (mRunning) {
    std::this_thread::sleep_for(100ms);
    StdSenderOutputCounters::Values lCurrCounters;
    {
      std::scoped_lock lLock(mCounters.mCountersLock);
      lCurrCounters = mCounters.mValues;
    }

    DDMON("stfsender", "stf_output.sent_count", lCurrCounters.mTotalSent.mCnt);
    DDMON("stfsender", "stf_output.sent_size", lCurrCounters.mTotalSent.mSize);
    DDMON("stfsender", "stf_output.rejected_cnt", lCurrCounters.mSchedulerStfRejectedCnt);
    DDMON("stfsender", "stf_output.missing_cnt", lCurrCounters.mTotalSent.mMissing);

    DDMON("stfsender", "buffered.stf_cnt", lCurrCounters.mBuffered.mCnt);
    DDMON("stfsender", "buffered.stf_size", lCurrCounters.mBuffered.mSize);

    DDMON("stfsender", "sending.stf_cnt", lCurrCounters.mInSending.mCnt);
    DDMON("stfsender", "sending.stf_size", lCurrCounters.mInSending.mSize);

    if (mStfCopyBuilder) {
      DDMON("stfsender", "stf_region.free", mStfCopyBuilder->freeData());
    }

    // Update consul params for standalone run
    if (mStfKeepThread.joinable()) {
      mDeletePercentage = mDiscoveryConfig->getUInt64Param(StandaloneStfDeleteChanceKey, StandaloneStfDeleteChanceDefault);
      mDeletePercentage = std::clamp(mDeletePercentage.load(), std::uint64_t(0), std::uint64_t(100));
      // Update the keep target
      mKeepTarget = mDiscoveryConfig->getUInt64Param(StandaloneStfDataBufferSizeMBKey, StandaloneStfDataBufferSizeMBDefault) << 20;
      mKeepTarget = std::clamp(mKeepTarget.load(), std::uint64_t(128ULL << 20), mBufferSize);
    }
  }

  DDDLOG("Exiting StfMonitoring thread");
}

} /* o2::DataDistribution */
