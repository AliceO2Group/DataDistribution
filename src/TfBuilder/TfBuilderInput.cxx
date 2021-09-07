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
#include <DataDistMonitoring.h>

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

  IDDLOG("Creating input channels. num_channels={} partition={}", mNumStfSenders, lStatus.partition().partition_id());

  const auto &lAaddress = lStatus.info().ip_address();

  auto &lSocketMap = *(lStatus.mutable_sockets()->mutable_map());

  // ZEROMQ high watermark on receive
  int lRcvBufSize = 50;
  {
    const auto lTfBZmqHwmVar = getenv("DATADIST_TF_ZMQ_RCVHWM");
    if (lTfBZmqHwmVar) {
      try {
        const int lTfBZmqHwm = std::stoi(lTfBZmqHwmVar);
        lRcvBufSize = std::max(lTfBZmqHwm, 4);
        IDDLOG("TfBuilderInput: RcvBufSize is set to {}. DATADIST_TF_ZMQ_RCVHWM={}", lRcvBufSize, lRcvBufSize);
      } catch (...) {
        EDDLOG("(Sub)TimeFrame source: DATADIST_TF_ZMQ_RCVHWM must be greater than 3. DATADIST_TF_ZMQ_RCVHWM={}",
          lTfBZmqHwmVar);
      }
    }
  }

  for (std::uint32_t lSocketIdx = 0; lSocketIdx < mNumStfSenders; lSocketIdx++) {

    std::string lAddress = "tcp://" + lAaddress + ":" + std::to_string(10000 + lSocketIdx);

    auto lNewChannel = std::make_unique<FairMQChannel>(
      "stf_sender_chan_" + std::to_string(lSocketIdx) ,  // name
      "pull",               // type
      "bind",               // method
      lAddress,             // address (TODO: this should only ever be ib interface)
      lTransportFactory
    );

    lNewChannel->UpdateRateLogging(1); // log each second
    lNewChannel->UpdateAutoBind(true); // make sure bind succeeds
    lNewChannel->UpdateRcvBufSize(lRcvBufSize); // make sure one sender does not advance too much
    lNewChannel->UpdateRcvKernelSize(2 << 20);
    lNewChannel->Init();

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

  // Start the pacer thread
  mReceivedData.start();
  mStfPacingThread = create_thread_member("tfb_pace", &TfBuilderInput::StfPacingThread, this);

  // Start the deserialize thread
  {
    std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
    mStfMergeMap.clear();

    mStfDeserThread = create_thread_member("tfb_deser", &TfBuilderInput::StfDeserializingThread, this);
  }

  // Start the merger
  mStfsForMerging.start();
  mStfMergerThread = create_thread_member("tfb_merge", &TfBuilderInput::StfMergerThread, this);


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

  //Wait for pacer thread
  mReceivedData.stop();
  if (mStfPacingThread.joinable()) {
    mStfPacingThread.join();
  }

  // Make sure the deserializer stopped
  {
    DDDLOG("TfBuilderInput::stop: Stopping the STF merger thread.");
    {
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
      mStfMergeMap.clear();
      IDDLOG("TfBuilderInput::stop: Merger queue emptied.");
      mStfMergerCondition.notify_all();
    }

    if (mStfDeserThread.joinable()) {
      mStfDeserThread.join();
    }
  }

  //Wait for merger thread
  mStfsForMerging.stop();
  if (mStfMergerThread.joinable()) {
    mStfMergerThread.join();
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

  // Deserialization object (stf ID)
  CoalescedHdrDataDeserializer lStfReceiver(mDevice.TfBuilderI());

  while (mState == RUNNING) {
    std::unique_ptr<std::vector<FairMQMessagePtr>> lStfData = std::make_unique<std::vector<FairMQMessagePtr>>();

    const std::int64_t lRet = lInputChan.Receive(*lStfData, 1000 /* ms */);

    switch (lRet) {
      case static_cast<std::int64_t>(fair::mq::TransferCode::timeout):
        continue;
        break;
      case static_cast<std::int64_t>(fair::mq::TransferCode::interrupted):
        if (mState == RUNNING) {
          IDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::interrupted pStfSenderId={}", pStfSenderId);
        }
        continue;
        break;
      case static_cast<std::int64_t>(fair::mq::TransferCode::error):
        EDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::error err={} errno={} error={}",
          int(lRet), errno, std::string(strerror(errno)));
        continue;
        break;
      default: // data or zero
        if (lRet <= 0) {
          WDDLOG_RL(1000, "STF receive failed. what=zero_size");
          continue;
        }
        break;
    }

    // move data to the dedicated region if required
    if (std::size_t(lRet) > mDevice.TfBuilderI().freeData()) {
      EDDLOG_GRL(1000, "TfBuilderInput::DataHandlerThread: Data region size too small for received data. received={} region_free={}",
        lRet, mDevice.TfBuilderI().freeData());
    }
    lStfReceiver.copy_to_region(*lStfData);

    // get Stf ID
    const SubTimeFrame::Header lStfHeader = lStfReceiver.peek_tf_header(*lStfData);
    const std::uint64_t lTfId = lStfHeader.mId;

    // signal in flight STF is finished (or error)
    mRpc->recordStfReceived(pStfSenderId, lTfId);

    // send to deserializer thread so that we can keep receiving
    mReceivedData.push(lTfId, lStfHeader.mOrigin, pStfSenderId, std::move(lStfData));
    lNumStfs++;
  }

  DDDLOG("Exiting input thread [{}]", pFlpIndex);
}

/// TF building pacing thread
void TfBuilderInput::StfPacingThread()
{
  // Deserialization object (stf ID)
  CoalescedHdrDataDeserializer lStfReceiver(mDevice.TfBuilderI());
  std::uint64_t lTopoStfId = 0;

  while (mState == RUNNING) {

    auto lData = mReceivedData.pop();
    if (lData == std::nullopt) {
      continue;
    }
    auto &lStfInfo = lData.value();

    assert (lStfInfo.mRecvStfdata);
    std::uint64_t lTfId = lStfInfo.mStfId;

    // Rename STF id if this is a Topological TF
    if (lStfInfo.mStfOrigin == SubTimeFrame::Header::Origin::eReadoutTopology) {
      // deserialize here to be able to rename the stf
      lStfInfo.mStf = std::move(lStfReceiver.deserialize(*lStfInfo.mRecvStfdata));
      lStfInfo.mRecvStfdata = nullptr;

      const std::uint64_t lNewTfId = ++lTopoStfId;
      DDDLOG_RL(5000, "Deserialized STF. stf_id={} new_id={}", lStfInfo.mStf->id(), lNewTfId);
      lStfInfo.mStf->updateId(lNewTfId);
      lTfId = lNewTfId;
      lStfInfo.mStfId = lTfId;
    }

    /// TODO: STF receive pacing
    // TfScheduler should manage memory of the region and not overcommit the TfBuilders

    { // Push the STF into the merger queue
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);

      if (lTfId > mMaxMergedTfId) {
        mStfMergeMap[lTfId].push_back(std::move(lStfInfo));
      } else {
        EDDLOG_RL(1000, "StfPacingThread: Received STF ID is larger than the last built STF. stfs_id={} stf_id={} last_stf_id={}",
          lStfInfo.mStfSenderId, lTfId, mMaxMergedTfId);
      }
    }
    // wake up the merging thread
    mStfMergerCondition.notify_one();
  }

  DDDLOG("Exiting StfPacingThread.");
}


void TfBuilderInput::deserialize_headers(std::vector<ReceivedStfMeta> &pStfs)
{
  // Deserialization object
  CoalescedHdrDataDeserializer lStfReceiver(mDevice.TfBuilderI());

  for (auto &lStfInfo : pStfs) {
    if (lStfInfo.mStf) {
      continue; // already deserialized
    }

    // deserialize the data
    lStfInfo.mStf = std::move(lStfReceiver.deserialize(*lStfInfo.mRecvStfdata));
    lStfInfo.mRecvStfdata = nullptr;
  }
}

// check if topological (S)TF
bool TfBuilderInput::is_topo_stf(const std::vector<ReceivedStfMeta> &pStfs) const
{
  if (pStfs.empty()) {
    return false;
  }

  if (pStfs.size() == 1 && pStfs.begin()->mStf) {
    if (pStfs.begin()->mStf->header().mOrigin == SubTimeFrame::Header::Origin::eReadoutTopology) {
      return true;
    }
  }

  if (pStfs.size() > 1 && pStfs.begin()->mStf) {
    if (pStfs.begin()->mStf->header().mOrigin == SubTimeFrame::Header::Origin::eReadoutTopology) {
      EDDLOG_RL(1000, "TfBuilderInput::is_topo_stf: multiple topological STFs with a same ID. stf_id={}",
        pStfs.begin()->mStf->id());
    }
  }

  return false;
}

/// FMQ->STF thread
/// This thread can block waiting on free O2 Header memory
void TfBuilderInput::StfDeserializingThread()
{
  // Deserialization object
  CoalescedHdrDataDeserializer lStfReceiver(mDevice.TfBuilderI());

  while (mState == RUNNING) {

    std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
    mStfMergerCondition.wait_for(lQueueLock, 500ms);

    if (mStfMergeMap.empty()) {
      continue;
    }

    // 1.1 deserialize all topological or completed TFs with the smallest ID
    {
      auto lMinTfInfo = mStfMergeMap.begin();
      const auto lStfId = lMinTfInfo->first;
      auto &lStfVector = lMinTfInfo->second;

      deserialize_headers(lStfVector);

      // 1.2 check if the oldest TF is completed or topological STF?
      if ((lStfVector.size() == mNumStfSenders) || is_topo_stf(lStfVector)) {
        mStfsForMerging.push(std::move(lStfVector));
        mStfMergeMap.erase(lStfId);
        continue;
      }
    }

    // 3.1 check out of order completions
    TimeFrameIdType lMergedStf = 0;
    for (auto &lStfInfosIter : mStfMergeMap) {
      const auto lStfId = lStfInfosIter.first;
      auto &lStfVector = lStfInfosIter.second;

      if (lStfVector.size() == mNumStfSenders) {
        lMergedStf = lStfId;
        break;
      }
    }

    // 3.2 Merge incomplete larger STFs
    if (lMergedStf != 0) {
      while (!mStfMergeMap.empty()) {
        // Get the TF with lowest ID
        auto lMinTfInfo = mStfMergeMap.begin();
        const auto lMinStfId = lMinTfInfo->first;
        auto &lStfVector = lMinTfInfo->second;

        // If stop if TF Id is after the just completed TF
        if (lMinStfId < lMergedStf) {
          break;
        }

        if (lStfVector.size() < mNumStfSenders) {
          WDDLOG_RL(1000, "StfDeserializingThread: Merging a TF with incomplete number of STFs. stf_num={} stfs_num={}",
            lStfVector.size(), mNumStfSenders);
        }

        deserialize_headers(lStfVector);
        mStfsForMerging.push(std::move(lStfVector));
        mStfMergeMap.erase(lMinStfId);
      }
    }
  }

  IDDLOG("Exiting stf deserializer thread.");
}

/// STF->TF Merger thread
void TfBuilderInput::StfMergerThread()
{
  using namespace std::chrono_literals;
  using hres_clock = std::chrono::high_resolution_clock;

  auto lRateStartTime = hres_clock::now();

  std::uint64_t lNumBuiltTfs = 0;

  while (mState == RUNNING) {

    auto lStfVectorOpt = mStfsForMerging.pop();
    if (lStfVectorOpt == std::nullopt) {
      continue;
    }
    auto &lStfVector = lStfVectorOpt.value();

    // merge the current TF!
    const std::chrono::duration<double, std::milli> lBuildDurationMs =
      lStfVector.rbegin()->mTimeReceived - lStfVector.begin()->mTimeReceived;

    // start from the first element (using it as the seed for the TF)
    std::unique_ptr<SubTimeFrame> lTf = std::move(lStfVector.begin()->mStf);
    if (!lTf) {
      EDDLOG("StfMergerThread: First Stf is null. (not deserialized?)");
      continue;
    }

    // Add the rest of STFs
    for (auto lStfIter = std::next(lStfVector.begin()); lStfIter != lStfVector.end(); ++lStfIter) {
      lTf->mergeStf(std::move(lStfIter->mStf));
    }
    lNumBuiltTfs++;

    const auto lTfId = lTf->id();

    mMaxMergedTfId = std::max(mMaxMergedTfId, lTfId);

    // account the size of received TF
    mRpc->recordTfBuilt(*lTf);

    DDDLOG_RL(1000, "Building of TF completed. tf_id={:d} duration_ms={} total_tf={:d}",
      lTfId, lBuildDurationMs.count(), lNumBuiltTfs);

    {
      const auto lNow = hres_clock::now();
      const auto lTfDur = std::chrono::duration<double>(lNow - lRateStartTime);
      lRateStartTime = lNow;
      const auto lRate = (1.0 / lTfDur.count());

      DDMON("tfbuilder", "tf_input.size", lTf->getDataSize());
      DDMON("tfbuilder", "tf_input.rate", lRate);
      DDMON("tfbuilder", "data_input.rate", (lRate * lTf->getDataSize()));

      DDMON("tfbuilder", "merge.receive_span_ms", lBuildDurationMs.count());
    }

    // Queue out the TF for consumption
    mDevice.queue(mOutStage, std::move(lTf));
  }

  IDDLOG("Exiting STF merger thread.");
}

} /* namespace o2::DataDistribution */
