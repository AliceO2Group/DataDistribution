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

#include "TfBuilderInput.h"
#include "TfBuilderRpc.h"
#include "TfBuilderDevice.h"

#include <TfSchedulerRpcClient.h>

#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameVisitors.h>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

bool TfBuilderInput::start(std::shared_ptr<ConsulTfBuilder> pConfig)
{
  // make max number of listening channels for the partition
  mDevice.GetConfig()->SetProperty<int>("io-threads", (int) std::min(std::thread::hardware_concurrency(), 32u));
  auto lTransportFactory = FairMQTransportFactory::CreateTransportFactory("zeromq", "", mDevice.GetConfig());

  auto &lStatus = pConfig->status();

  std::uint32_t lNumStfSenders;
  if (!mRpc->TfSchedRpcCli().NumStfSendersInPartitionRequest(lNumStfSenders)) {
    EDDLOG("gRPC error: cannot reach scheduler. scheduler_ep={}", mRpc->TfSchedRpcCli().getEndpoint());
    return false;
  }

  if (lNumStfSenders == 0 || lNumStfSenders == std::uint32_t(-1)) {
    EDDLOG("gRPC error: number of StfSenders in partition: {}." , lNumStfSenders);
    return false;
  }

  mNumStfSenders = lNumStfSenders;

  IDDLOG("Creating input channels. num_channels={} partition={}",
    mNumStfSenders, lStatus.partition().partition_id());

  const auto &lAaddress = lStatus.info().ip_address();

  auto &lSocketMap = *(lStatus.mutable_sockets()->mutable_map());

  for (std::uint32_t lSocketIdx = 0; lSocketIdx < mNumStfSenders; lSocketIdx++) {

    std::string lAddress = "tcp://" + lAaddress + ":" + std::to_string(10000 + lSocketIdx);

    auto lNewChannel = std::make_unique<FairMQChannel>(
      "stf_sender_chan_" + std::to_string(lSocketIdx) ,  // name
      "pull",               // type
      "bind",               // method
      lAddress,             // address (TODO: this should only ever be ib interface)
      lTransportFactory
    );

    lNewChannel->Init();

    lNewChannel->UpdateRateLogging(1); // log each second

    lNewChannel->UpdateAutoBind(true); // make sure bind succeeds

    if (!lNewChannel->BindEndpoint(lAddress)) {
      EDDLOG("Cannot bind channel to a free port! Check permissions. bind_address={}", lAddress);
      return false;
    }

    if (!lNewChannel->Validate()) {
      EDDLOG("Channel validation failed! Exiting!");
      return false;
    }

    // save channel addresses to configuration
    auto &lSocket = lSocketMap[lSocketIdx];
    lSocket.set_idx(lSocketIdx);
    lSocket.set_endpoint(lAddress);

    mStfSenderChannels.push_back(std::move(lNewChannel));
  }

  if (pConfig->write()) {
    IDDLOG("New channels created. Discovery configuration written.");
  } else {
    IDDLOG("New channels created. Discovery configuration writing failed!");
    return false;
  }


  // Connect all StfSenders
  TfBuilderConnectionResponse lConnResult;
  do {
    IDDLOG("Requesting StfSender connections from the TfSchedulerInstance.");

    lConnResult.Clear();
    if (!mRpc->TfSchedRpcCli().TfBuilderConnectionRequest(lStatus, lConnResult)) {
      EDDLOG("RPC error: Request for StfSender connection failed.");
      return false;
    }

    if (lConnResult.status() == ERROR_STF_SENDERS_NOT_READY) {
      WDDLOG("StfSenders are not ready. Retrying...");
      std::this_thread::sleep_for(1s);
      continue;
    }

    if (lConnResult.status() == ERROR_PARTITION_TERMINATING) {
      WDDLOG("Partition is terminating. Stopping.");
      return false;
    }

    if (lConnResult.status() != OK) {
      EDDLOG("Request for StfSender connection failed. scheduler_error={}",
        TfBuilderConnectionStatus_Name(lConnResult.status()));
      return false;
    }

    // connection successful
    break;

   } while(true);

  // Update socket map with peer information
  for (auto &[lSocketIdx, lStfSenderId] : lConnResult.connection_map()) {
    IDDLOG("Connected StfSender id={} socket_idx={}", lStfSenderId, lSocketIdx);

    // save socket peers to configuration
    lSocketMap[lSocketIdx].set_peer_id(lStfSenderId);
  }
  pConfig->write();


  // Start all the threads
  mState = RUNNING;

  // Start the deserialize thread
  mReceivedData.start();
  mStfDeserThread = create_thread_member("tfb_deser", &TfBuilderInput::StfDeserializingThread, this);

  // Start the merger
  {
    std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
    mStfMergeMap.clear();
    mStfCount = 0;

    // start the merger thread
    mStfMergerThread = create_thread_member("tfb_merge", &TfBuilderInput::StfMergerThread, this);
  }

  // start all input threads
  assert(mInputThreads.size() == 0);

  for (auto &[lSocketIdx, lStfSenderId] : lConnResult.connection_map()) {
    char lThreadName[128];
    std::snprintf(lThreadName, 127, "tfb_in_%03u", (unsigned)lSocketIdx);
    lThreadName[15] = '\0';

    mInputThreads.try_emplace(
      lStfSenderId,
      create_thread_member(lThreadName, &TfBuilderInput::DataHandlerThread, this, lSocketIdx, lStfSenderId)
    );
  }

  // finally start accepting TimeFrames
  mRpc->startAcceptingTfs();

  return true;
}

void TfBuilderInput::stop(std::shared_ptr<ConsulTfBuilder> pConfig)
{
  // first stop accepting TimeFrames
  mRpc->stopAcceptingTfs();

  mState = TERMINATED;

  // Disconnect all input channels
  // RPC: Send disconnect request to scheduler
  {
    StatusResponse lResult;
    auto &lStatus = pConfig->status();

    if (mRpc->TfSchedRpcCli().TfBuilderDisconnectionRequest(lStatus, lResult)) {
      IDDLOG("RPC Request for StfSender disconnect successful.");
    } else {
      EDDLOG("RPC error: Request for StfSender disconnect failed!");
    }
  }

  // Wait for input threads to stop
  DDDLOG("TfBuilderInput::stop: Waiting for input threads to terminate.");
  for (auto& lIdThread : mInputThreads) {
    if (lIdThread.second.joinable())
      lIdThread.second.join();
  }
  mInputThreads.clear();
  DDDLOG("TfBuilderInput::stop: All input threads terminated.");

  // disconnect and close the sockets
  for (auto &lFmqChannelPtr : mStfSenderChannels) {
    if (!lFmqChannelPtr->IsValid()) {
      WDDLOG("TfBuilderInput::stop: Socket not found for channel. socket_ep={}",
        lFmqChannelPtr->GetAddress());
      continue;
    }
    lFmqChannelPtr->GetSocket().SetLinger(0);
    lFmqChannelPtr->GetSocket().Close();
  }
  mStfSenderChannels.clear();
  IDDLOG("TfBuilderInput::stop: All input channels are closed.");

  //Wait for deserializer thread
  mReceivedData.stop();
  if (mStfDeserThread.joinable()) {
    mStfDeserThread.join();
  }

  // Make sure the merger stopped
  {
    DDDLOG("TfBuilderInput::stop: Stopping the STF merger thread.");
    {
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
      mStfMergeMap.clear();
      IDDLOG("TfBuilderInput::stop: Merger queue emptied.");
      mStfMergerCondition.notify_all();
    }

    if (mStfMergerThread.joinable()) {
      mStfMergerThread.join();
    }
  }
  DDDLOG("TfBuilderInput::stop: Merger thread stopped.");
  IDDLOG("TfBuilderInput: Teardown complete.");
}

/// Receiving thread
void TfBuilderInput::DataHandlerThread(const std::uint32_t pFlpIndex, const std::string pStfSenderId)
{
  std::uint64_t lNumStfs = 0;

  DataDistLogger::SetThreadName(fmt::format("Receiver[{}]", pStfSenderId));
  DDDLOG("Starting receiver thread for StfSender[{}]", pStfSenderId);

  // Reference to the input channel
  auto& lInputChan = *mStfSenderChannels[pFlpIndex];

  while (mState == RUNNING) {
    std::unique_ptr<std::vector<FairMQMessagePtr>> lStfData = std::make_unique<std::vector<FairMQMessagePtr>>();

    const std::int64_t ret = lInputChan.Receive(*lStfData, 1000 /* ms */);
    // timeout ?
    if (ret == -2) {
      continue;
    }

    if (ret < 0) {
      IDDLOG_GRL(1000, "STF receive failed err={} errno={} error={}", ret, errno, std::string(strerror(errno)));
      continue;
    }

    // send to deserializer thread so that we can keep receiving
    mReceivedData.push(std::move(ReceivedStfMeta(pStfSenderId, std::move(lStfData))));
    lNumStfs++;
  }

  IDDLOG("Exiting input thread [{}]", pFlpIndex);
}

/// FMQ->STF thread
void TfBuilderInput::StfDeserializingThread()
{
  std::uint64_t lNumStfs = 0;
  // Deserialization object
  CoalescedHdrDataDeserializer lStfReceiver(mDevice.TfBuilderI());

  while (mState == RUNNING) {

    auto lData = mReceivedData.pop();
    if (lData == std::nullopt) {
      continue;
    }
    auto &lStfInfo = lData.value();

    assert (lStfInfo.mRecvStfdata);

    // try to deserialize
    lStfInfo.mStf = lStfReceiver.deserialize(*lStfInfo.mRecvStfdata);
    if (!lStfInfo.mStf) {
      continue;
    }

    const TimeFrameIdType lTfId = lStfInfo.mStf->id();
    lNumStfs++;

    // Rename TF id if this is a Topological TF
    std::uint64_t lNewTfId = 0;
    if (mRpc->getTopologicalTfId(lData->mStfSenderId, lTfId, lNewTfId)) {
      DDDLOG_RL(5000, "Deserialized STF. stf_id={} new_id={} total={}", lTfId, lNewTfId, lNumStfs);
      lStfInfo.mStf->updateId(lNewTfId);
    } else {
      DDDLOG_RL(5000, "Deserialized STF. stf_id={} total={}", lTfId, lNumStfs);
    }

    {
      // Push the STF into the merger queue
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);

      mStfMergeMap[lTfId].push_back(std::move(lStfInfo));
      mStfCount++;

      if (mStfMergeMap[lTfId].size() == mNumStfSenders) {
        lQueueLock.unlock();
        mStfMergerCondition.notify_one();
      }
    }
  }

  IDDLOG("Exiting stf deserializer thread.");
}

/// STF->TF Merger thread
void TfBuilderInput::StfMergerThread()
{
  using namespace std::chrono_literals;

  std::uint64_t lNumBuiltTfs = 0;

  while (mState == RUNNING) {

    std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);

    if (mStfCount < mNumStfSenders) {
      if (std::cv_status::timeout == mStfMergerCondition.wait_for(lQueueLock, 500ms)) {
        continue;
      }
    }

    if (mStfCount < mNumStfSenders) {
      continue;
    }

    for (auto &lStfInfoIt : mStfMergeMap) {
      auto &lStfMetaVec = lStfInfoIt.second;
      const auto lStfId = lStfInfoIt.first;

      if (lStfMetaVec.size() < mNumStfSenders) {
        continue;
      }

      if (lStfMetaVec.size() > mNumStfSenders) {
        EDDLOG("StfMerger: number of STFs is larger than expected. stf_id={:d} num_stfs={:d} num_stf_senders={:d}",
          lStfId, lStfMetaVec.size(), mNumStfSenders);
      }

      // merge the current TF!
      const auto lBuildDurationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        lStfMetaVec.rbegin()->mTimeReceived - lStfMetaVec.begin()->mTimeReceived);

      // start from the first element (using it as the seed for the TF)
      std::unique_ptr<SubTimeFrame> lTf = std::move(lStfMetaVec.begin()->mStf);

      for (auto lStfIter = std::next(lStfMetaVec.begin()); lStfIter != lStfMetaVec.end(); ++lStfIter) {
          // Add them all up
          lTf->mergeStf(std::move(lStfIter->mStf));
      }

      lNumBuiltTfs++;
      DDDLOG_RL(1000, "Building of TF completed. tf_id={:d} duration_ms={} total_tf={:d}",
        lStfId, lBuildDurationMs.count(), lNumBuiltTfs);

      // remove consumed STFs from the merge queue
      mStfMergeMap.erase(lStfId);
      mStfCount -= mNumStfSenders;

      // account the size of received TF
      mRpc->recordTfBuilt(*lTf);

      // Queue out the TF for consumption
      mDevice.queue(mOutStage, std::move(lTf));

      // break from the for loop and try again
      break;
    }
  }

  IDDLOG("Exiting STF merger thread.");
}

} /* namespace o2::DataDistribution */
