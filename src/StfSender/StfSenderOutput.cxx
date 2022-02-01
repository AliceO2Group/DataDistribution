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

  { // TODO: only for testing
    const auto lStfsBuffSizeVar = getenv("DATADIST_STFS_BUFFER_SIZE");
    if (lStfsBuffSizeVar) {
      try {
        auto lBuffSize = std::stoull(lStfsBuffSizeVar);
        lBuffSize = std::max(lBuffSize, (8ULL << 30));
        lBuffSize = std::min(lBuffSize, (48ULL << 30));

        WDDLOG("StfSender buffer size override. size={}", lBuffSize);
        mBufferSize = lBuffSize;
      } catch (...) {
        EDDLOG("StfSender buffer size override failed. DATADIST_STFS_BUFFER_SIZE={}", lStfsBuffSizeVar);
      }
    }
  }

  // create a socket and connect
  mDevice.GetConfig()->SetProperty<int>("io-threads", (int) std::min(std::thread::hardware_concurrency(), 20u));
  mZMQTransportFactory = FairMQTransportFactory::CreateTransportFactory("zeromq", "", mDevice.GetConfig());

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

  // signal threads to stop
  for (auto& lIdOutputIt : mOutputMap) {
    lIdOutputIt.second.mStfQueue->stop();
  }

  // wait for threads to exit
  for (auto& lIdOutputIt : mOutputMap) {
    if (lIdOutputIt.second.mThread.joinable()) {
      lIdOutputIt.second.mThread.join();
    }
  }

  {
    std::scoped_lock lLock(mOutputMapLock);
    mOutputMap.clear();
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

StfSenderOutput::ConnectStatus StfSenderOutput::connectTfBuilder(const std::string &pTfBuilderId,
                                                                 const std::string &pEndpoint)
{
  // Check if connection already exists
  {
    std::scoped_lock lLock(mOutputMapLock);

    if (mOutputMap.count(pTfBuilderId) > 0) {
      EDDLOG("StfSenderOutput::connectTfBuilder: TfBuilder is already connected. tfb_id={}", pTfBuilderId);
      return eEXISTS;
    }
  }

  auto lChanName = "tf_builder_" + pTfBuilderId;
  std::replace(lChanName.begin(), lChanName.end(),'.', '_');

  auto lNewChannel = std::make_unique<FairMQChannel>(
    lChanName ,              // name
    "push",                  // type
    "connect",               // method
    pEndpoint,               // address (TODO: this should only ever be the IB interface)
    mZMQTransportFactory
  );

  lNewChannel->UpdateSndBufSize(12);
  lNewChannel->Init();

  if (!lNewChannel->Validate()) {
    EDDLOG("Channel validation failed when connecting. tfb_id={} ep={}", pTfBuilderId, pEndpoint);
    return eCONNERR;
  }

  if (!lNewChannel->ConnectEndpoint(pEndpoint)) {
    EDDLOG("Cannot connect a new cannel. ep={}", pEndpoint);
    return eCONNERR;
  }

  // create all resources for the connection
  {
    std::scoped_lock lLock(mOutputMapLock);

    char lThreadName[128];
    std::snprintf(lThreadName, 127, "to_%s", pTfBuilderId.c_str());
    lThreadName[15] = '\0'; // kernel limitation

    auto lSer = std::make_unique<IovSerializer>(*lNewChannel);

    mOutputMap.try_emplace(
      pTfBuilderId,
      OutputChannelObjects {
        pEndpoint,
        std::move(lNewChannel),
        std::move(lSer),
        std::make_unique<ConcurrentFifo<std::unique_ptr<SubTimeFrame>>>(),
        // Note: this thread will try to access this same map. The MapLock will prevent races
        create_thread_member(lThreadName, &StfSenderOutput::DataHandlerThread, this, pTfBuilderId)
      }
    );
  }

  // update our connection status
  auto &lSocketMap = *(mDiscoveryConfig->status().mutable_sockets()->mutable_map());
  auto &lEntry = lSocketMap[pTfBuilderId];
  lEntry.set_peer_id(pTfBuilderId);
  lEntry.set_peer_endpoint(pEndpoint);
  mDiscoveryConfig->write();

  return eOK;
}

bool StfSenderOutput::disconnectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint)
{
  // find and remove from map
  OutputChannelObjects lOutputObj;
  {
    std::scoped_lock lLock(mOutputMapLock);
    auto lIt = mOutputMap.find(pTfBuilderId);

    if (lIt == mOutputMap.end()) {
      WDDLOG("StfSenderOutput::disconnectTfBuilder: TfBuilder was not connected. tfb_id={}", pTfBuilderId);
      return false; // TODO: ERRORCODE
    }

    if (!lEndpoint.empty()) {
      // Check if the endpoint matches if provided
      if (lEndpoint != lIt->second.mTfBuilderEndpoint) {
        WDDLOG("StfSenderOutput::disconnectTfBuilder: TfBuilder connected with a different endpoint"
          "current_ep={} requested_ep={}", lOutputObj.mTfBuilderEndpoint, lEndpoint);
        return false;
      }
    }

    lOutputObj = std::move(lIt->second);
    mOutputMap.erase(pTfBuilderId);
  }

  // stop and teardown everything
  DDDLOG("StfSenderOutput::disconnectTfBuilder: Stopping sending thread. tfb_id={}", pTfBuilderId);
  lOutputObj.mStfQueue->stop();
  lOutputObj.mStfSerializer->stop();
  if (lOutputObj.mThread.joinable()) {
    lOutputObj.mThread.join();
  }
  DDDLOG("StfSenderOutput::disconnectTfBuilder: Stopping sending channel. tfb_id={}", pTfBuilderId);

  // update our connection status
  auto &lSocketMap = *(mDiscoveryConfig->status().mutable_sockets()->mutable_map());
  lSocketMap.erase(pTfBuilderId);
  mDiscoveryConfig->write();

  DDDLOG("StfSenderOutput::disconnectTfBuilder tfb_id={}", pTfBuilderId);
  return true;
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
    StdSenderOutputCounters lCounters;
    {
      std::scoped_lock lLock(mCountersLock);
      mCounters.mBuffered.mSize += lStfSize;
      mCounters.mBuffered.mCnt += 1;

      lCounters = mCounters;
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
  std::scoped_lock lLock(mOutputMapLock, mScheduledStfMapLock);

  const auto lStfIter = mScheduledStfMap.find(pStfId);
  // verify we have the STF: we can have
  if (lStfIter == mScheduledStfMap.end()) {
    if (pTfBuilderId != "-1") {
      pRes.set_status(StfDataResponse::DATA_DROPPED_UNKNOWN);
      EDDLOG_GRL(1000, "sendStfToTfBuilder: TfBuilder requested non-existing STF. stf_id={}", pStfId);
    } else {
      pRes.set_status(StfDataResponse::DATA_DROPPED_SCHEDULER);
    }
  } else if (pTfBuilderId == "-1") { // check if it is drop request from the scheduler
    pRes.set_status(StfDataResponse::DATA_DROPPED_SCHEDULER);
    mDropQueue.push(std::move(lStfIter->second));
    mScheduledStfMap.erase(lStfIter);
  } else {
    auto lTfBuilderIter = mOutputMap.find(pTfBuilderId);
    if (lTfBuilderIter == mOutputMap.end()) {
      pRes.set_status(StfDataResponse::TF_BUILDER_UNKNOWN);
      mDropQueue.push(std::move(lStfIter->second));
      mScheduledStfMap.erase(lStfIter);
      EDDLOG_GRL(1000, "sendStfToTfBuilder: TfBuilder not known to StfSender. tfb_id={}", pTfBuilderId);
      return;
    }

    // all is well, schedule the stf and cleanup.
    // we clean the buffer when data is sent
    pRes.set_status(StfDataResponse::OK);

    const auto lStfSize = lStfIter->second->getDataSize();

    {
      std::scoped_lock lCntLock(mCountersLock);
      mCounters.mInSending.mSize += lStfSize;
      mCounters.mInSending.mCnt += 1;
    }

    auto lStfNode = mScheduledStfMap.extract(lStfIter);
    lTfBuilderIter->second.mStfQueue->push(std::move(lStfNode.mapped()));

    if (lTfBuilderIter->second.mStfQueue->size() > 50) {
      WDDLOG_RL(1000, "SendToTfBuilder: STF queue backlog. queue_size={}", lTfBuilderIter->second.mStfQueue->size());
    }
  }
}

/// Sending thread
void StfSenderOutput::DataHandlerThread(const std::string pTfBuilderId)
{
  DDDLOG("StfSenderOutput[{}]: Starting the thread", pTfBuilderId);
  // decrease the priority
#if defined(__linux__)
  if (nice(2)) {}
#endif

  IovSerializer *lStfSerializer = nullptr;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> *lInputStfQueue = nullptr;
  {
    // get the thread data. MapLock prevents races on the map operation
    std::scoped_lock lLock(mOutputMapLock);
    auto &lOutData = mOutputMap.at(pTfBuilderId);

    lStfSerializer = lOutData.mStfSerializer.get();
    lInputStfQueue = lOutData.mStfQueue.get();
  }
  assert(lInputStfQueue != nullptr && lInputStfQueue->is_running());

  std::uint64_t lNumSentStfs = 0;

  std::optional<std::unique_ptr<SubTimeFrame>> lStfOpt;

  while ((lStfOpt = lInputStfQueue->pop()) != std::nullopt) {
    std::unique_ptr<SubTimeFrame> lStf = std::move(lStfOpt.value());
    lStfOpt.reset();

    const auto lStfId = lStf->id();
    const auto lStfSize = lStf->getDataSize();

    DDDLOG_GRL(5000, "Sending an STF to TfBuilder. stf_id={} tfb_id={} stf_size={} total_sent_stf={}",
      lStfId, pTfBuilderId, lStfSize, lNumSentStfs);

    try {
      lStfSerializer->serialize(std::move(lStf));
    } catch (std::exception &e) {
      if (mDevice.IsRunningState()){
        EDDLOG("StfSenderOutput[{}]: exception on send, what={}", pTfBuilderId, e.what());
      } else {
        IDDLOG("StfSenderOutput[{}](NOT RUNNING): exception on send. what={}", pTfBuilderId, e.what());
      }
      break;
    }

    // update buffer status
    lNumSentStfs += 1;

    StdSenderOutputCounters lCounters;
    {
      std::scoped_lock lCntLock(mCountersLock);
      mCounters.mBuffered.mSize -= lStfSize;
      mCounters.mBuffered.mCnt -= 1;
      mCounters.mInSending.mSize -= lStfSize;
      mCounters.mInSending.mCnt -= 1;
      mCounters.mTotalSent.mSize += lStfSize;
      mCounters.mTotalSent.mCnt += 1;

      lCounters = mCounters;
    }

    if (lCounters.mInSending.mCnt > 100) {
      DDDLOG_RL(2000, "DataHandlerThread: Number of buffered STFs. tfb_id={} num_stfs={} num_stf_total={} size_stf_total={}",
        pTfBuilderId, lInputStfQueue->size(), lCounters.mInSending.mCnt, lCounters.mInSending.mSize);
    }

    DDMON("stfsender", "stf_output.stf_id", lStfId);
  }

  DDDLOG("Exiting StfSenderOutput[{}]", pTfBuilderId);
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
      std::scoped_lock lLock(mCountersLock);
      mCounters.mBuffered.mSize -= lStfSize;
      mCounters.mBuffered.mCnt -= 1;
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

  StdSenderOutputCounters lPrevCounters;
  {
    std::scoped_lock lLock(mCountersLock);
    lPrevCounters = mCounters;
  }
  std::chrono::high_resolution_clock::time_point lLastSent = std::chrono::high_resolution_clock::now();

  while (mRunning) {
    std::this_thread::sleep_for(100ms);
    StdSenderOutputCounters lCurrCounters;
    {
      std::scoped_lock lLock(mCountersLock);
      lCurrCounters = mCounters;
    }
    const auto lNow = std::chrono::high_resolution_clock::now();

    DDMON("stfsender", "stf_output.sent_count", lCurrCounters.mTotalSent.mCnt);
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
