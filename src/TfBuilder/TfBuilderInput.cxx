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
    DDLOG(fair::Severity::ERROR) << "RPC error: cannot reach scheduler on : " << mRpc->TfSchedRpcCli().getEndpoint();
    return false;
  }

  if (lNumStfSenders == 0 || lNumStfSenders == std::uint32_t(-1)) {
    DDLOG(fair::Severity::ERROR) << "RPC error: number of StfSenders in partition: " << lNumStfSenders;
    return false;
  }

  mNumStfSenders = lNumStfSenders;

  DDLOG(fair::Severity::INFO) << "Creating " << mNumStfSenders << " input channels for partition " << lStatus.partition().partition_id();

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
      DDLOG(fair::Severity::ERROR) << "Cannot bind channel to a free port! Check user permissions. Bind address: " << lAddress;
      return false;
    }

    if (!lNewChannel->Validate()) {
      DDLOG(fair::Severity::ERROR) << "Channel validation failed! Exiting...\n";
      return false;
    }

    // save channel addresses to configuration
    auto &lSocket = lSocketMap[lSocketIdx];
    lSocket.set_idx(lSocketIdx);
    lSocket.set_endpoint(lAddress);

    mStfSenderChannels.emplace_back(std::move(lNewChannel));
  }

  DDLOG(fair::Severity::INFO) << "New channels created. Writing Discovery configuration...";
  pConfig->write();


  // Connect all StfSenders
  TfBuilderConnectionResponse lConnResult;
  do {
    DDLOG(fair::Severity::INFO) << "Requesting connections from StfSenders from the TfSchedulerInstance";

    lConnResult.Clear();
    if (!mRpc->TfSchedRpcCli().TfBuilderConnectionRequest(lStatus, lConnResult)) {
      DDLOG(fair::Severity::ERROR) << "RPC error: Request for StfSender connection failed .";
      return false;
    }

    if (lConnResult.status() == ERROR_STF_SENDERS_NOT_READY) {
      DDLOG(fair::Severity::WARN) << "StfSenders are not ready. Retrying...";
      std::this_thread::sleep_for(1s);
      continue;
    }

    if (lConnResult.status() != 0) {
      DDLOG(fair::Severity::ERROR) << "Request for StfSender connection failed, scheduler error: " << TfBuilderConnectionStatus_Name(lConnResult.status());
      return false;
    }

    // connection successful
    break;

   } while(true);

  // Update socket map with peer information
  for (auto &[lSocketIdx, lStfSenderId] : lConnResult.connection_map()) {
    DDLOG(fair::Severity::INFO) << "Connected StfSender ID[" << lStfSenderId << "] to input socket index " << lSocketIdx;

    // save socket peers to configuration
    lSocketMap[lSocketIdx].set_peer_id(lStfSenderId);
  }
  pConfig->write();

  mState = RUNNING;

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
    char tname[128];
    fmt::format_to(tname, "tfb_input_{}", lSocketIdx);

    mInputThreads.try_emplace(
      lStfSenderId,
      create_thread_member(tname, &TfBuilderInput::DataHandlerThread, this, lSocketIdx)
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
      DDLOG(fair::Severity::INFO) << "RPC Request for StfSender disconnect successful.";
    } else {
      DDLOG(fair::Severity::ERROR) << "RPC error: Request for StfSender disconnect failed .";
    }
  }

  // Wait for input threads to stop
  DDLOG(fair::Severity::DEBUG) << "TfBuilderInput::stop: Waiting for input threads to terminate.";
  for (auto& lIdThread : mInputThreads) {
    if (lIdThread.second.joinable())
      lIdThread.second.join();
  }
  mInputThreads.clear();
  DDLOG(fair::Severity::DEBUG) << "TfBuilderInput::stop: All input threads terminated.";

  // disconnect and close the sockets
  for (auto &lFmqChannelPtr : mStfSenderChannels) {
    if (!lFmqChannelPtr->IsValid()) {
      DDLOG(fair::Severity::WARN) << "TfBuilderInput::stop: Socket not found for channel " << lFmqChannelPtr->GetAddress();
      continue;
    }
    lFmqChannelPtr->GetSocket().SetLinger(0);
    lFmqChannelPtr->GetSocket().Close();
  }
  mStfSenderChannels.clear();
  DDLOG(fair::Severity::DEBUG) << "TfBuilderInput::stop: All input channels are closed.";

  // Make sure the merger stopped
  {
    DDLOG(fair::Severity::INFO) << "TfBuilderInput::stop: Stopping the STF merger thread.";
    {
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
      mStfMergeMap.clear();
      DDLOG(fair::Severity::INFO) << "TfBuilderInput::stop: Merger queue emptied.";
      mStfMergerCondition.notify_all();
    }

    if (mStfMergerThread.joinable()) {
      mStfMergerThread.join();
      mStfMergerThread = {};
    }
  }
  DDLOG(fair::Severity::DEBUG) << "TfBuilderInput::stop: Merger thread stopped.";

  DDLOG(fair::Severity::INFO) << "TfBuilderInput: Teardown complete...";
}


// TODO: add thread that waits on: getNewTfBuildingRequest()
// -> and sends the data request to StfBuilders.



/// Receiving thread
void TfBuilderInput::DataHandlerThread(const std::uint32_t pFlpIndex)
{
  std::uint64_t lNumStfs = 0;

  DataDistLogger::SetThreadName(fmt::format("Receiver[{}]", pFlpIndex));
  DDLOGF(fair::Severity::TRACE, "Starting receiver thread for StfSender[{}]", pFlpIndex);

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
    lNumStfs++;

    const TimeFrameIdType lTfId = lStf->header().mId;

    DDLOGF_RL(5000, fair::Severity::DEBUG, "Received STF. flp_idx={} stf_id={} total={}",
      pFlpIndex, lTfId, lNumStfs);

    {
      // Push the STF into the merger queue
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);

      mStfMergeMap[lTfId].emplace_back(std::move(lStf));
      mStfCount++;

      if (mStfMergeMap[lTfId].size() == mNumStfSenders) {
        lQueueLock.unlock();
        mStfMergerCondition.notify_one();
      }
    }
  }

  DDLOG(fair::Severity::INFO) << "Exiting input thread[" << pFlpIndex << "]...";
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
        DDLOGF(fair::Severity::ERROR,
          "StfMerger: number of STFs is larger than expected. stf_id={:d} num_stfs={:d} num_stf_senders={:d}",
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
      DDLOGF_RL(500, fair::Severity::DEBUG, "Building of TF completed. tf_id={:d} duration_ms={} total_tf={:d}",
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

  DDLOG(fair::Severity::INFO) << "Exiting STF merger thread...";
}
}
} /* namespace o2::DataDistribution */
