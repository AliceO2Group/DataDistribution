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

#if defined(__linux__)
#include <unistd.h>
#endif

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

void StfSenderOutput::start(std::shared_ptr<ConsulStfSender> pDiscoveryConfig)
{
  assert(pDiscoveryConfig);
  mDiscoveryConfig = pDiscoveryConfig;
  mRunning = true;

  // Get DD buffer size option
  auto lBufferSizeMB = mDiscoveryConfig->getUInt64Param(StfBufferSizeMBKey, StfBufferSizeMBValue);
  auto lOptBufferSize = lBufferSizeMB << 20;
  if (lOptBufferSize != mBufferSize) {
    lOptBufferSize = std::clamp(lOptBufferSize, std::uint64_t(8ULL << 30), std::uint64_t(64ULL << 30));
    mBufferSize = lOptBufferSize;
    WDDLOG("StfSender buffer size override. size={}", lOptBufferSize);
  }

  // create a socket and connect
  mDevice.GetConfig()->SetProperty<int>("io-threads", (int) std::min(std::thread::hardware_concurrency(), 20u));
  mZMQTransportFactory = FairMQTransportFactory::CreateTransportFactory("zeromq", "", mDevice.GetConfig());

  // create output
  mOutputFairmq = std::make_unique<StfSenderOutputFairMQ>(pDiscoveryConfig, mCounters);
  mOutputFairmq->start(mZMQTransportFactory);

  // create stf drop thread
  mStfDropThread = create_thread_member("stfs_drop", &StfSenderOutput::StfDropThread, this);

  // create scheduler thread
  mSchedulerThread = create_thread_member("stfs_sched", &StfSenderOutput::StfSchedulerThread, this);

  // create monitoring thread
  mMonitoringThread = create_thread_member("stfs_mon", &StfSenderOutput::StfMonitoringThread, this);
}

void StfSenderOutput::stop()
{
  mRunning = false;
  // stop the scheduler: on pipeline interrupt
  if (mSchedulerThread.joinable()) {
    mSchedulerThread.join();
  }

  // stop the drop queue
  mDropQueue.stop();
  if (mStfDropThread.joinable()) {
    mStfDropThread.join();
  }

  if (mDevice.standalone()) {
    return;
  }

  // signal backends to stop
  mOutputFairmq->stop();

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
  return mOutputFairmq->connectTfBuilder(pTfBuilderId, pEndpoint);
}

bool StfSenderOutput::disconnectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint)
{
  return mOutputFairmq->disconnectTfBuilder(pTfBuilderId, lEndpoint);
}

void StfSenderOutput::StfSchedulerThread()
{
  // Notifies the scheduler about stfs
  DDDLOG("StfSchedulerThread: Starting.");

  // increase the priority
#if defined(__linux__)
  if (nice(-10)) {}
#endif

  std::unique_ptr<SubTimeFrame> lStf;

  while ((lStf = mPipelineI.dequeue(eSenderIn)) != nullptr) {
    const auto lStfId = lStf->id();
    const auto lStfSize = lStf->getDataSize();

    // update buffer sizes
    StdSenderOutputCounters::Values lCounters;
    {
      std::scoped_lock lLock(mCounters.mCountersLock);
      // check for missing Stfs
      mCounters.mValues.mTotalSent.mMissing += (lStfId - mLastStfId - 1);
      mLastStfId = lStfId;

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
      case SubTimeFrame::Header::Origin::eNull:
      {
        lStfInfo.set_stf_source(StfSource::EMPTY);
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
      auto [it, ins] = mScheduledStfMap.try_emplace(lStfId, std::move(lStf));
      if (!ins) {
        (void)it;
        EDDLOG_RL(500, "StfSchedulerThread: Stf is already scheduled! Skipping the duplicate. stf_id={}", lStfId);
        mDropQueue.push(std::move(lStf));
        continue;
      }
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
            mDropQueue.push(std::move(lStfIter->second));
            mScheduledStfMap.erase(lStfIter);
          }
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
    mDropQueue.push(std::move(lStfIter->second));
    mScheduledStfMap.erase(lStfIter);
  } else {

    // extract the Stf
    auto lStf = std::move(lStfIter->second);
    const auto lStfSize = lStf->getDataSize();
    mScheduledStfMap.erase(lStfIter);

    // send to output backend
    const bool lOk = mOutputFairmq->sendStfToTfBuilder(pTfBuilderId, std::move(lStf));
    if (!lOk) {
      pRes.set_status(StfDataResponse::TF_BUILDER_UNKNOWN);
      mDropQueue.push(std::move(lStf));
      EDDLOG_GRL(1000, "sendStfToTfBuilder: TfBuilder not known to StfSender. tfb_id={}", pTfBuilderId);
      return;
    }

    // update status and counters
    pRes.set_status(StfDataResponse::OK);
    {
      std::scoped_lock lCntLock(mCounters.mCountersLock);
      mCounters.mValues.mInSending.mSize += lStfSize;
      mCounters.mValues.mInSending.mCnt += 1;
    }
  }
}


/// Drop thread
void StfSenderOutput::StfDropThread()
{
  DDDLOG("Starting DataDropThread thread.");
  // decrease the priority
#if defined(__linux__)
  if (nice(5)) {}
#endif

  std::uint64_t lNumDroppedStfs = 0;

  while (mRunning) {
    std::unique_ptr<SubTimeFrame> lStf;
    if (!mDropQueue.pop(lStf)) {
      break;
    }

    const auto lStfSize = lStf->getDataSize();

    DDDLOG_GRL(5000, "Dropping an STF. stf_id={} stf_size={} total_dropped_stf={}", lStf->header().mId,
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

void StfSenderOutput::StfMonitoringThread()
{
  DDDLOG("Starting StfMonitoring thread.");

  // decrease the priority
#if defined(__linux__)
  if (nice(10)) {}
#endif

  StdSenderOutputCounters::Values lPrevCounters;
  {
    std::scoped_lock lLock(mCounters.mCountersLock);
    lPrevCounters = mCounters.mValues;
  }
  std::chrono::high_resolution_clock::time_point lLastSent = std::chrono::high_resolution_clock::now();

  while (mRunning) {
    std::this_thread::sleep_for(100ms);
    StdSenderOutputCounters::Values lCurrCounters;
    {
      std::scoped_lock lLock(mCounters.mCountersLock);
      lCurrCounters = mCounters.mValues;
    }
    const auto lNow = std::chrono::high_resolution_clock::now();

    DDMON("stfsender", "stf_output.sent_count", lCurrCounters.mTotalSent.mCnt);
    DDMON("stfsender", "stf_output.missing_cnt", lCurrCounters.mTotalSent.mMissing);
    DDMON("stfsender", "stf_output.sent_size", lCurrCounters.mTotalSent.mSize);

    DDMON("stfsender", "buffered.stf_cnt", lCurrCounters.mBuffered.mCnt);
    DDMON("stfsender", "buffered.stf_size", lCurrCounters.mBuffered.mSize);

    DDMON("stfsender", "sending.stf_cnt", lCurrCounters.mInSending.mCnt);
    DDMON("stfsender", "sending.stf_size", lCurrCounters.mInSending.mSize);

    const std::chrono::duration<double> lDuration = lNow - lLastSent;
    const double lSentStfs = double(lCurrCounters.mTotalSent.mCnt - lPrevCounters.mTotalSent.mCnt);

    if (lSentStfs > 0) {
      const auto lStfRate = lSentStfs / std::max(0.000001, lDuration.count());
      const auto lStfSize = (lCurrCounters.mTotalSent.mSize - lPrevCounters.mTotalSent.mSize) / lSentStfs;
      DDMON("stfsender", "stf_output.rate", lStfRate);
      DDMON("stfsender", "stf_output.size", lStfSize);
      DDMON("stfsender", "data_output.rate", (lStfRate * lStfSize));
    } else {
      DDMON("stfsender", "stf_output.rate", 0);
      DDMON("stfsender", "stf_output.size", 0);
      DDMON("stfsender", "data_output.rate", 0);
    }

    lPrevCounters = lCurrCounters;
    lLastSent = lNow;
  }

  DDDLOG("Exiting StfMonitoring thread");
}

} /* o2::DataDistribution */
