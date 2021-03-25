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

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

void StfSenderOutput::start(std::shared_ptr<ConsulStfSender> pDiscoveryConfig)
{
  assert(pDiscoveryConfig);
  mDiscoveryConfig = pDiscoveryConfig;

  { // TODO: remove me
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

  // create stf drop thread
  mStfDropThread = create_thread_member("stfs_drop", &StfSenderOutput::StfDropThread, this);

  // create scheduler thread
  mSchedulerThread = create_thread_member("stfs_sched", &StfSenderOutput::StfSchedulerThread, this);
}

void StfSenderOutput::stop()
{
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
      WDDLOG("StfSenderOutput::connectTfBuilder: TfBuilder is already connected. tfb_id={}", pTfBuilderId);
      return eEXISTS; // TODO: ERRORCODE
    }
  }

  // create a socket and connect
  auto transportFactory = FairMQTransportFactory::CreateTransportFactory("zeromq");

  auto lChanName = "tf_builder_" + pTfBuilderId;
  std::replace(lChanName.begin(), lChanName.end(),'.', '_');

  auto lNewChannel = std::make_unique<FairMQChannel>(
    lChanName ,              // name
    "push",                  // type
    "connect",               // method
    pEndpoint,               // address (TODO: this should only ever be the IB interface)
    transportFactory
  );

  lNewChannel->UpdateSndBufSize(10);
  lNewChannel->Init();
  lNewChannel->UpdateRateLogging(1); // log each second

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

    char tname[128];
    fmt::format(tname, "to_{}", pTfBuilderId);

    mOutputMap.try_emplace(
      pTfBuilderId,
      OutputChannelObjects {
        pEndpoint,
        std::move(lNewChannel),
        std::make_unique<ConcurrentFifo<std::unique_ptr<SubTimeFrame>>>(),
        // Note: this thread will try to access this same map. The MapLock will prevent races
        create_thread_member(tname, &StfSenderOutput::DataHandlerThread, this, pTfBuilderId)
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
  IDDLOG("StfSenderOutput::disconnectTfBuilder: Stopping sending thread. tfb_id={}", pTfBuilderId);
  lOutputObj.mStfQueue->stop();
  if (lOutputObj.mThread.joinable()) {
    lOutputObj.mThread.join();
  }
  DDDLOG("StfSenderOutput::disconnectTfBuilder: Stopping sending channel. tfb_id={}", pTfBuilderId);
  if (lOutputObj.mChannel->IsValid() ) {
    lOutputObj.mChannel->GetSocket().Close();
  }

  // update our connection status
  auto &lSocketMap = *(mDiscoveryConfig->status().mutable_sockets()->mutable_map());
  lSocketMap.erase(pTfBuilderId);
  mDiscoveryConfig->write();

  IDDLOG("StfSenderOutput::disconnectTfBuilder tfb_id={}", pTfBuilderId);
  return true;
}

void StfSenderOutput::StfSchedulerThread()
{
  DDDLOG("StfSchedulerThread: Starting.");
  // Notifies the scheduler about stfs
  std::unique_ptr<SubTimeFrame> lStf;

  while ((lStf = mDevice.dequeue(eSenderIn)) != nullptr) {
    const auto lStfId = lStf->header().mId;
    const auto lStfSize = lStf->getDataSize();

    if (mDevice.standalone()) {
      continue;
    }

    if (!mDevice.TfSchedRpcCli().is_ready()) {
      EDDLOG_RL(1000, "StfSchedulerThread: TfScheduler gRPC connection is not ready stf_id={}", lStfId);
      continue;
    }

    DDDLOG_RL(5000, "StfSchedulerThread: scheduling stf_id={}", lStfId);

    StfSenderStfInfo lStfInfo;
    SchedulerStfInfoResponse lSchedResponse;

    // move the stf into triage map (before notifying the scheduler to avoid races)
    {
      std::scoped_lock lLock(mScheduledStfMapLock);

      auto [it, ins] = mScheduledStfMap.try_emplace(lStfId, std::move(lStf));
      if (!ins) {
        (void)it;
        EDDLOG_RL(500, "StfSchedulerThread: Stf already scheduled! Skipping the duplicate. stf_id={}", lStfId);
        continue;
      }

      // update buffer size
      mCounters.mBufferedStfSize += lStfSize;
      mCounters.mBufferedStfCnt += 1;

      // send current state of the buffers
      lStfInfo.mutable_stfs_info()->set_buffer_size(mBufferSize);
      lStfInfo.mutable_stfs_info()->set_buffer_used(mCounters.mBufferedStfSize);
      lStfInfo.mutable_stfs_info()->set_num_buffered_stfs(mCounters.mBufferedStfCnt);
    }

    // Send STF info to scheduler
    {
      const auto &lStatus = mDiscoveryConfig->status();
      lStfInfo.mutable_info()->CopyFrom(lStatus.info());
      lStfInfo.mutable_partition()->CopyFrom(lStatus.partition());
      lStfInfo.set_stf_id(lStfId);
      lStfInfo.set_stf_size(lStfSize);

      const auto lSentOK = mDevice.TfSchedRpcCli().StfSenderStfUpdate(lStfInfo, lSchedResponse);
      // check if the scheduler rejected the data
      if (!lSentOK || (lSchedResponse.status() != SchedulerStfInfoResponse::OK)) {
        {
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
      DDDLOG_RL(5000, "Sent STF announce, stf_id={} stf_size={}", lStfId, lStfInfo.stf_size());
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
    return;
  }

  // check if it is drop request from the scheduler
  if (pTfBuilderId == "-1") {
    pRes.set_status(StfDataResponse::DATA_DROPPED_SCHEDULER);
    mDropQueue.push(std::move(lStfIter->second));
    mScheduledStfMap.erase(lStfIter);
    return;
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

    mCounters.mBufferedStfSizeSending += lStfIter->second->getDataSize();
    mCounters.mBufferedStfCntSending += 1;

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
  IDDLOG("StfSenderOutput[{}]: Starting the thread", pTfBuilderId);

  FairMQChannel *lOutputChan = nullptr;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> *lInputStfQueue = nullptr;
  {
    // get the thread data. MapLock prevents races on the map operation
    std::scoped_lock lLock(mOutputMapLock);
    auto &lOutData = mOutputMap.at(pTfBuilderId);

    lOutputChan = lOutData.mChannel.get();
    lInputStfQueue = lOutData.mStfQueue.get();
  }
  assert(lOutputChan != nullptr && lOutputChan->IsValid());
  assert(lInputStfQueue != nullptr && lInputStfQueue->is_running());

  std::uint64_t lNumSentStfs = 0;

  CoalescedHdrDataSerializer lStfSerializer(*lOutputChan);
  std::optional<std::unique_ptr<SubTimeFrame>> lStfOpt;

  while ((lStfOpt = lInputStfQueue->pop()) != std::nullopt) {
    std::unique_ptr<SubTimeFrame> lStf = std::move(lStfOpt.value());
    lStfOpt.reset();

    const auto lStfSize = lStf->getDataSize();

    DDDLOG_GRL(5000, "Sending an STF to TfBuilder. stf_id={} tfb_id={} stf_size={} total_sent_stf={}",
      lStf->header().mId, pTfBuilderId, lStfSize, lNumSentStfs);

    try {
      lStfSerializer.serialize(std::move(lStf));
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

    {
      std::scoped_lock lLock(mScheduledStfMapLock);
      mCounters.mBufferedStfSize -= lStfSize;
      mCounters.mBufferedStfCnt -= 1;
      mCounters.mBufferedStfSizeSending -= lStfSize;
      mCounters.mBufferedStfCntSending -= 1;

      if (mCounters.mBufferedStfCntSending > 100) {
        DDDLOG_RL(2000, "DataHandlerThread: Number of buffered STFs. tfb_id={} num_stfs={} num_stf_total={} size_stf_total={}",
          pTfBuilderId, lInputStfQueue->size(), mCounters.mBufferedStfCntSending, mCounters.mBufferedStfSizeSending);
      }
    }
  }

  IDDLOG("Exiting StfSenderOutput[{}]", pTfBuilderId);
}

/// Sending thread
void StfSenderOutput::StfDropThread()
{
  DDDLOG("Starting DataDropThread thread.");
  std::uint64_t lNumDroppedStfs = 0;
  std::optional<std::unique_ptr<SubTimeFrame>> lStfOpt;

  while ((lStfOpt = mDropQueue.pop()) != std::nullopt) {
    std::unique_ptr<SubTimeFrame> lStf = std::move(lStfOpt.value());
    lStfOpt.reset();
    const auto lStfSize = lStf->getDataSize();

    DDDLOG_GRL(5000, "Dropping an STF. stf_id={} stf_size={} total_dropped_stf={}", lStf->header().mId,
      lStfSize, lNumDroppedStfs);

    // delete the data
    lStf.reset();
    // update buffer status
    lNumDroppedStfs += 1;
    {
      std::scoped_lock lLock(mScheduledStfMapLock);
      mCounters.mBufferedStfSize -= lStfSize;
      mCounters.mBufferedStfCnt -= 1;
    }
  }

  DDDLOG("Exiting DataDropThread thread");
}

}
} /* o2::DataDistribution */
