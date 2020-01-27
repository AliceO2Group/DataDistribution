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

#include <FairMQLogger.h>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

bool TfBuilderInput::start(std::shared_ptr<ConsulTfBuilder> pConfig)
{
  // make max number of listening channels for the partition
  auto transportFactory = FairMQTransportFactory::CreateTransportFactory("zeromq");

  auto &lStatus = pConfig->status();

  std::uint32_t lNumStfSenders;
  if (!mRpc->TfSchedRpcCli().NumStfSendersInPartitionRequest(lNumStfSenders)) {
    LOG(ERROR) << "RPC error: cannot reach scheduler on : " << mRpc->TfSchedRpcCli().getEndpoint();
    return false;
  }

  if (lNumStfSenders == 0 || lNumStfSenders == std::uint32_t(-1)) {
    LOG(ERROR) << "RPC error: number of StfSenders in partition: " << lNumStfSenders;
    return false;
  }

  mNumStfSenders = lNumStfSenders;

  LOG(INFO) << "Creating " << mNumStfSenders << " input channels for partition " << lStatus.partition().partition_id();

  const auto &lAaddress = lStatus.info().ip_address();

  auto &lSocketMap = *(lStatus.mutable_sockets()->mutable_map());

  for (std::uint32_t lSocketIdx = 0; lSocketIdx < mNumStfSenders; lSocketIdx++) {

    std::string lAddress = "tcp://" + lAaddress + ":" + std::to_string(10000 + lSocketIdx);

    auto lNewChannel = std::make_unique<FairMQChannel>(
      "stf_sender_chan_" + std::to_string(lSocketIdx) ,  // name
      "pair",               // type
      "bind",               // method
      lAddress,             // address (TODO: this should only ever be ib interface)
      transportFactory
    );

    lNewChannel->Init();

    lNewChannel->UpdateRateLogging(1); // log each second

    lNewChannel->UpdateAutoBind(true); // make sure bind succeeds

    if (!lNewChannel->BindEndpoint(lAddress)) {
      LOG (ERROR) << "Cannot bind channel to a free port! Check user permissions. Bind address: " << lAddress;
      return false;
    }

    if (!lNewChannel->Validate()) {
      LOG (ERROR) << "Channel validation failed! Exiting...\n";
      return false;
    }

    // save channel addresses to configuration
    auto &lSocket = lSocketMap[lSocketIdx];
    lSocket.set_idx(lSocketIdx);
    lSocket.set_endpoint(lAddress);

    mStfSenderChannels.emplace_back(std::move(lNewChannel));
  }

  LOG(INFO) << "New channels created. Writing Discovery configuration...";
  pConfig->write();


  // Connect all StfSenders
  TfBuilderConnectionResponse lConnResult;
  do {
    LOG(INFO) << "Requesting connections from StfSenders from the TfSchedulerInstance";

    lConnResult.Clear();
    if (!mRpc->TfSchedRpcCli().TfBuilderConnectionRequest(lStatus, lConnResult)) {
      LOG(ERROR) << "RPC error: Request for StfSender connection failed .";
      return false;
    }

    if (lConnResult.status() == ERROR_STF_SENDERS_NOT_READY) {
      LOG(WARN) << "StfSenders are not ready. Retrying...";
      std::this_thread::sleep_for(1s);
      continue;
    }

    if (lConnResult.status() != 0) {
      LOG(ERROR) << "Request for StfSender connection failed, scheduler error: " << TfBuilderConnectionStatus_Name(lConnResult.status());
      return false;
    }

    // connection successful
    break;

   } while(true);

  // Update socket map with peer information
  for (auto &[lSocketIdx, lStfSenderId] : lConnResult.connection_map()) {
    LOG(INFO) << "Connected StfSender ID[" << lStfSenderId << "] to input socket index " << lSocketIdx;

    // save socket peers to configuration
    lSocketMap[lSocketIdx].set_peer_id(lStfSenderId);
  }
  pConfig->write();

  mState = RUNNING;

  // Start the merger
  {
    std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
    mStfMergeQueue.clear();

    // start the merger thread
    mStfMergerThread = std::thread(&TfBuilderInput::StfMergerThread, this);
  }

  // start all input threads
  assert(mInputThreads.size() == 0);

  for (auto &[lSocketIdx, lStfSenderId] : lConnResult.connection_map()) {
    mInputThreads.try_emplace(lStfSenderId, std::thread(&TfBuilderInput::DataHandlerThread, this, lSocketIdx));
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
      LOG(INFO) << "RPC Request for StfSender disconnect successful.";
    } else {
      LOG(ERROR) << "RPC error: Request for StfSender disconnect failed .";
    }
  }

  // Wait for input threads to stop
  LOG(DEBUG) << "TfBuilderInput::stop: Waiting for input threads to terminate.";
  for (auto& lIdThread : mInputThreads) {
    if (lIdThread.second.joinable())
      lIdThread.second.join();
  }
  mInputThreads.clear();
  LOG(DEBUG) << "TfBuilderInput::stop: All input threads terminated.";

  // disconnect and close the sockets
  for (auto &lFmqChannelPtr : mStfSenderChannels) {
    if (!lFmqChannelPtr->IsValid()) {
      LOG(WARN) << "TfBuilderInput::stop: Socket not found for channel " << lFmqChannelPtr->GetAddress();
      continue;
    }
    lFmqChannelPtr->GetSocket().SetLinger(0);
    lFmqChannelPtr->GetSocket().Close();
  }
  mStfSenderChannels.clear();
  LOG(DEBUG) << "TfBuilderInput::stop: All input channels are closed.";

  // Make sure the merger stopped
  {
    LOG(INFO) << "TfBuilderInput::stop: Stopping the Stf merger thread.";
    {
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
      mStfMergeQueue.clear();
      LOG(INFO) << "TfBuilderInput::stop: Merger queue emptied.";
      mStfMergerCondition.notify_all();
    }

    if (mStfMergerThread.joinable()) {
      mStfMergerThread.join();
      mStfMergerThread = {};
    }
  }
  LOG(DEBUG) << "TfBuilderInput::stop: Merger thread stopped.";

  LOG(INFO) << "TfBuilderInput: Teardown complete...";
}


// TODO: add thread that waits on: getNewTfBuildingRequest()
// -> and sends the data request to StfBuilders.



/// Receiving thread
void TfBuilderInput::DataHandlerThread(const std::uint32_t pFlpIndex)
{
  LOG(INFO) << "Starting input thread for StfSender[" << pFlpIndex << "]...";

  // Reference to the input channel
  auto& lInputChan = *mStfSenderChannels[pFlpIndex];

  // Deserialization object
  InterleavedHdrDataDeserializer lStfReceiver;

  while (mState == RUNNING) {
    // receive a STF
    std::unique_ptr<SubTimeFrame> lStf = lStfReceiver.deserialize(lInputChan);
    if (!lStf) {
     // timeout
     continue;
    }

    const TimeFrameIdType lTfId = lStf->header().mId;

    {
      static thread_local std::atomic_uint64_t sNumStfs = 0;
      if (++sNumStfs % 88 == 0)
      LOG(DEBUG) << "Received Stf from flp " << pFlpIndex << " with id " << lTfId << ", total: " << sNumStfs;
    }

    {
      // Push the STF into the merger queue
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
      mStfMergeQueue.emplace(std::make_pair(lTfId, std::move(lStf)));

      // Notify the Merger if enough inputs are collected
      // NOW:  Merge STFs if exactly |FLP| chunks have been received
      //       or a next TF started arriving (STFs from previous delayed or not
      //       available)
      // TODO: Find out exactly how many STFs is arriving.
      if (mStfMergeQueue.size() >= mNumStfSenders){
        lQueueLock.unlock();
        mStfMergerCondition.notify_one();
      }
    }
  }

  LOG(INFO) << "Exiting input thread[" << pFlpIndex << "]...";
}

/// STF->TF Merger thread
void TfBuilderInput::StfMergerThread()
{
  using namespace std::chrono_literals;

  while (mState == RUNNING) {

    std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);

    if (mStfMergeQueue.empty()) {
      // wait for the signal
      if (std::cv_status::timeout == mStfMergerCondition.wait_for(lQueueLock, 500ms)) {
        continue; // should exit?
      }
    }

    // check for spurious signaling
    if (mStfMergeQueue.empty()) {
      continue;
    }

    // check the merge queue for partial TFs first
    const SubTimeFrameIdType lTfId = mStfMergeQueue.begin()->first;

    // Case 1: a full TF can be merged
    if (mStfMergeQueue.count(lTfId) == mNumStfSenders) {

      auto lStfRange = mStfMergeQueue.equal_range(lTfId);
      assert(std::distance(lStfRange.first, lStfRange.second) == mNumStfSenders);

      auto lStfCount = 1UL; // start from the first element
      std::unique_ptr<SubTimeFrame> lTf = std::move(lStfRange.first->second);

      for (auto lStfIter = std::next(lStfRange.first); lStfIter != lStfRange.second; ++lStfIter) {
        // Add them all up
        lTf->mergeStf(std::move(lStfIter->second));
        lStfCount++;
      }

      if (lStfCount < mNumStfSenders) {
        LOG(WARN) << "STF MergerThread: merging incomplete TF[" << lTf->header().mId << "]: contains "
                  << lStfCount << " instead of " << mNumStfSenders << " SubTimeFrames";
      }

      // remove consumed STFs from the merge queue
      mStfMergeQueue.erase(lStfRange.first, lStfRange.second);

      // account the size of received TF
      mRpc->recordTfBuilt(*lTf);

      // Queue out the TF for consumption
      mDevice.queue(mOutStage, std::move(lTf));

    } else if (mStfMergeQueue.size() > (50 * mNumStfSenders)) {
      // FIXME: for now, discard incomplete TFs
      LOG(WARN) << "Unbounded merge queue size: " << mStfMergeQueue.size();

      const auto lDroppedStfs = mStfMergeQueue.count(lTfId);

      mStfMergeQueue.erase(lTfId);

      LOG(WARN) << "Dropping oldest incomplete TF... (" << lDroppedStfs << " STFs)";
    }
  }

  LOG(INFO) << "Exiting STF merger thread...";
}
}
} /* namespace o2::DataDistribution */
