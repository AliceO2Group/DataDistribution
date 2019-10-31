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

#include <FairMQLogger.h>

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

  std::scoped_lock lLock(mOutputMapLock);

  // create scheduler thread
  mSchedulerThread = std::thread(&StfSenderOutput::StfSchedulerThread, this);

  if (mDevice.standalone()) {
    return;
  }
}

void StfSenderOutput::stop()
{
  // stop the scheduler: on pipeline interrupt

  if (mSchedulerThread.joinable()) {
    mSchedulerThread.join();
  }

  if (mDevice.standalone()) {
    return;
  }

  // signal threads to stop
  for (auto& lIdOutputIt : mOutputMap) {
    lIdOutputIt.second.mRunning->store(false);
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
      LOG(WARN) << "StfSenderOutput::connectTfBuilder: TfBuilder is already connected: " << pTfBuilderId;
      return eEXISTS; // TODO: ERRORCODE
    }
  }

  // create a socket and connect
  auto transportFactory = FairMQTransportFactory::CreateTransportFactory("zeromq", fair::mq::tools::Uuid(), mDevice.GetConfig());

  auto lNewChannel = std::make_unique<FairMQChannel>(
    "tf_builder_" + pTfBuilderId ,  // name
    "pair",                  // type
    "connect",               // method
    pEndpoint,               // address (TODO: this should only ever be ib interface)
    transportFactory
  );

  lNewChannel->Init();
  lNewChannel->UpdateRateLogging(1); // log each second

  if (!lNewChannel->Validate()) {
    LOG (ERROR) << "Channel validation failed when connecting to " << pTfBuilderId << "endpoint: " << pEndpoint;
    return eCONNERR;
  }

  if (!lNewChannel->ConnectEndpoint(pEndpoint)) {
    LOG (ERROR) << "Cannot connect a new channel to address: " << pEndpoint;
    return eCONNERR;
  }

  // create all resources for the connection
  {
    std::scoped_lock lLock(mOutputMapLock);

    mOutputMap.try_emplace(
      pTfBuilderId,
      OutputChannelObjects {
        pEndpoint,
        std::move(lNewChannel),
        std::make_unique<ConcurrentFifo<std::unique_ptr<SubTimeFrame>>>(),
        // Note: this thread will try to access this same map. The MapLock will prevent races
        std::thread(&StfSenderOutput::DataHandlerThread, this, pTfBuilderId),
        std::make_unique<std::atomic_bool>(true) // running
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
      LOG(WARN) << "StfSenderOutput::disconnectTfBuilder: TfBuilder was not connected " << pTfBuilderId;
      return false; // TODO: ERRORCODE
    }

    if (!lEndpoint.empty()) {
      // Check if the endpoint matches if provided
      if (lEndpoint != lIt->second.mTfBuilderEndpoint) {
        LOG(WARN) << "StfSenderOutput::disconnectTfBuilder: TfBuilder connected with different endpoint" << lOutputObj.mTfBuilderEndpoint
                  << ", requested: " << lEndpoint;
        return false;
      }
    }

    lOutputObj = std::move(lIt->second);
    mOutputMap.erase(pTfBuilderId);
  }

  // stop and teardown everything
  LOG(INFO) << "StfSenderOutput::disconnectTfBuilder: Stopping sending thread for " << pTfBuilderId;
  lOutputObj.mRunning->store(false);
  lOutputObj.mStfQueue->stop();
  if (lOutputObj.mThread.joinable()) {
    lOutputObj.mThread.join();
  }
  LOG(DEBUG) << "StfSenderOutput::disconnectTfBuilder: Stopping sending channel " << pTfBuilderId;
  if (lOutputObj.mChannel->IsValid() ) {
    lOutputObj.mChannel->GetSocket().Close();
  }

  // update our connection status
  auto &lSocketMap = *(mDiscoveryConfig->status().mutable_sockets()->mutable_map());
  lSocketMap.erase(pTfBuilderId);
  mDiscoveryConfig->write();

  LOG(INFO) << "StfSenderOutput::disconnectTfBuilder: Disconnected from " << pTfBuilderId;
  return true;
}


void StfSenderOutput::StfSchedulerThread()
{
  LOG(INFO) << "StfSchedulerThread: Starting...";
  // queue the Stf to the appropriate EPN queue
  std::unique_ptr<SubTimeFrame> lStf;

  // wait for the device to go into RUNNING state
  mDevice.WaitForRunningState();

  while ((lStf = mDevice.dequeue(eSenderIn)) != nullptr) {

    // take a ref for later use
    SubTimeFrame &lStfRef = *lStf;
    const auto lStfId = lStfRef.header().mId;

    {
      static std::uint64_t sStfSchedulerThread = 0;
      if (++sStfSchedulerThread % 10 == 0) {
        LOG(DEBUG) << "StfSchedulerThread: stf id: " << lStfId << ", total: " << sStfSchedulerThread;
      }
    }

    if (mDevice.standalone()) {
      // Do not forward STFs
      continue;
    }

    // move the stf into triage map (before notifying the scheduler to avoid races)
    {
      std::scoped_lock lLock(mScheduledStfMapLock);
      auto [it, ins] = mScheduledStfMap.try_emplace(lStfId, std::move(lStf));
      if (!ins) {
        (void)it;
        LOG(ERROR) << "Stf with id: " << lStfId << " already scheduled! Skipping the duplicate.";
        continue;
      }
    }

    // Send STF info to scheduler
    {
      const auto &lStatus = mDiscoveryConfig->status();

      StfSenderStfInfo lStfInfo;
      SchedulerStfInfoResponse lSchedResponse;

      *lStfInfo.mutable_info() = lStatus.info();
      *lStfInfo.mutable_partition() = lStatus.partition();

      lStfInfo.set_stf_id(lStfId);
      lStfInfo.set_stf_size(lStfRef.getDataSize());

      mDevice.TfSchedRpcCli().StfSenderStfUpdate(lStfInfo, lSchedResponse);

      {
        static std::uint64_t sNumStfSentUpdates = 0;
        if (++sNumStfSentUpdates % 2 == 0) {
          LOG(DEBUG) << "Sent STF announce, id: " << lStfId << ", size: " << lStfInfo.stf_size() << ", total: " << sNumStfSentUpdates;
        }
      }

      // check if the scheduler rejected the data
      if (lSchedResponse.status() != SchedulerStfInfoResponse::OK) {
        LOG (INFO) << "TfScheduler rejected the Stf announce: " << lStfId
                   << ", reason: " << SchedulerStfInfoResponse_StfInfoStatus_Name(lSchedResponse.status());

        // remove from the scheduling map
        std::scoped_lock lLock(mScheduledStfMapLock);
        if (mScheduledStfMap.erase(lStfId) == 1) {
          // Decrement buffered STF count
          mDevice.stfCountDecFetch();
        }
      }
    }
  }

  LOG(INFO) << "StfSchedulerThread: Exiting...";
}

void StfSenderOutput::sendStfToTfBuilder(const std::uint64_t pStfId, const std::string &pTfBuilderId, StfDataResponse &pRes)
{
  assert(! pTfBuilderId.empty());

  std::scoped_lock lLock(mOutputMapLock, mScheduledStfMapLock);

  // check if it is drop request from the scheduler
  if (pTfBuilderId == "-1") {
    {
      static std::atomic_uint64_t sNumDropRequests = 0;
      if (++sNumDropRequests % 50 == 0) {
        LOG(DEBUG) << "Scheduler requested drop of STF: " << pStfId << ", total requests: " << sNumDropRequests;
      }
    }
    pRes.set_status(StfDataResponse::DATA_DROPPED_SCHEDULER);
    if (mScheduledStfMap.erase(pStfId) == 1) {
      // Decrement buffered STF count
      mDevice.stfCountDecFetch();
    }
    return;
  } else {
    auto lStfIter = mScheduledStfMap.find(pStfId);
    if (lStfIter == mScheduledStfMap.end()) {
      pRes.set_status(StfDataResponse::DATA_DROPPED_UNKNOWN);
      return;
    }

    auto lTfBuilderIter = mOutputMap.find(pTfBuilderId);
    if (lTfBuilderIter == mOutputMap.end()) {
      pRes.set_status(StfDataResponse::TF_BUILDER_UNKNOWN);
      return;
    }

    // all is well, schedule the stf and cleanup
    lTfBuilderIter->second.mStfQueue->push(std::move(lStfIter->second));
    mScheduledStfMap.erase(lStfIter);

    pRes.set_status(StfDataResponse::OK);
  }
}

/// Sending thread
void StfSenderOutput::DataHandlerThread(const std::string pTfBuilderId)
{
  LOG(INFO) << "StfSenderOutput[" << pTfBuilderId << "]: Starting the thread";

  FairMQChannel *lOutputChan = nullptr;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> *lInputStfQueue = nullptr;
  std::atomic_bool *lRunning = nullptr;
  {
    // get the thread data. MapLock prevents races on the map operation
    std::scoped_lock lLock(mOutputMapLock);
    auto &lOutData = mOutputMap.at(pTfBuilderId);

    lOutputChan = lOutData.mChannel.get();
    lInputStfQueue = lOutData.mStfQueue.get();
    lRunning = lOutData.mRunning.get();
  }
  assert(lOutputChan != nullptr && lOutputChan->IsValid());
  assert(lInputStfQueue != nullptr && lInputStfQueue->is_running());
  assert(lRunning != nullptr && (lRunning->load() == true));

  InterleavedHdrDataSerializer lStfSerializer(*lOutputChan);

  while (lRunning->load()) {
    std::unique_ptr<SubTimeFrame> lStf;

    if (!lInputStfQueue->pop(lStf)) {
      LOG(INFO) << "StfSenderOutput[" << pTfBuilderId << "]: STF queue drained. Exiting.";
      break;
    }

    {
      static std::atomic_uint64_t sNumSentStfs = 0;
      if (++sNumSentStfs % 100 == 0) {
        LOG(DEBUG) << "Sending Stf to " << pTfBuilderId << " with id " << lStf->header().mId << ", total sent: " << sNumSentStfs;
      }
    }

    try {
      lStfSerializer.serialize(std::move(lStf));
    } catch (std::exception &e) {

      if (mDevice.IsRunningState()){
        LOG(ERROR) << "StfSenderOutput[" << pTfBuilderId << "]: exception on send: " << e.what();
      } else {
        LOG(INFO) << "StfSenderOutput[" << pTfBuilderId << "](NOT RUNNING): exception on send: " << e.what();
      }

      break;
    }


    // Decrement buffered STF count
    mDevice.stfCountDecFetch();

  }

  LOG(INFO) << "Exiting StfSenderOutput[" << pTfBuilderId << "]";
}

}
} /* o2::DataDistribution */
