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

#include "TfBuilderInputFairMQ.h"
#include "TfBuilderInputDefs.h"
#include "TfBuilderRpc.h"

#include <TfSchedulerRpcClient.h>

#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameVisitors.h>

#include <DataDistMonitoring.h>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

bool TfBuilderInputFairMQ::start(std::shared_ptr<ConsulTfBuilder> pConfig, std::shared_ptr<FairMQTransportFactory> pZMQTransportFactory)
{
  auto &lStatus = pConfig->status();

  std::uint32_t lNumStfSenders;
  if (!mRpc->TfSchedRpcCli().NumStfSendersInPartitionRequest(lNumStfSenders)) {
    WDDLOG_RL(5000, "gRPC error: cannot reach scheduler. scheduler_ep={}", mRpc->TfSchedRpcCli().getEndpoint());
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
      pZMQTransportFactory
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


  // start all input threads
  assert(mInputThreads.size() == 0);

  for (auto &[lSocketIdx, lStfSenderId] : lConnResult.connection_map()) {
    char lThreadName[128];
    std::snprintf(lThreadName, 127, "tfb_in_%03u", (unsigned)lSocketIdx);
    lThreadName[15] = '\0';

    mInputThreads.try_emplace(
      lStfSenderId,
      create_thread_member(lThreadName, &TfBuilderInputFairMQ::DataHandlerThread, this, lSocketIdx, lStfSenderId)
    );
  }

  return true;
}

void TfBuilderInputFairMQ::stop(std::shared_ptr<ConsulTfBuilder> pConfig)
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
  DDDLOG("TfBuilderInputFairMQ::stop: Waiting for input threads to terminate.");
  for (auto& lIdThread : mInputThreads) {
    if (lIdThread.second.joinable())
      lIdThread.second.join();
  }
  mInputThreads.clear();
  DDDLOG("TfBuilderInputFairMQ::stop: All input threads terminated.");

  // disconnect and close the sockets
  for (auto &lFmqChannelPtr : mStfSenderChannels) {
    if (!lFmqChannelPtr->IsValid()) {
      WDDLOG("TfBuilderInputFairMQ::stop: Socket not found for channel. socket_ep={}",
        lFmqChannelPtr->GetAddress());
      continue;
    }
    lFmqChannelPtr->GetSocket().SetLinger(0);
  }
  mStfSenderChannels.clear();

  DDDLOG("TfBuilderInputFairMQ::stop: All input channels are closed.");
}

/// Receiving thread
void TfBuilderInputFairMQ::DataHandlerThread(const std::uint32_t pFlpIndex, const std::string pStfSenderId)
{
  std::uint64_t lNumStfs = 0;

  DataDistLogger::SetThreadName(fmt::format("Receiver[{}]", pStfSenderId));
  DDDLOG("Starting receiver thread for StfSender[{}]", pStfSenderId);

  // Reference to the input channel
  auto& lInputChan = *mStfSenderChannels[pFlpIndex];

  // Deserialization object (stf ID)
  IovDeserializer lStfReceiver(mTimeFrameBuilder);

  while (mState == RUNNING) {

    std::unique_ptr<std::vector<FairMQMessagePtr>> lStfData;


    lStfData = std::make_unique<std::vector<FairMQMessagePtr>>();

    const std::int64_t lRet = lInputChan.Receive(*lStfData, 1000 /* ms */);
    if (static_cast<std::int64_t>(fair::mq::TransferCode::timeout) == lRet) {
      continue;
    } else if (static_cast<std::int64_t>(fair::mq::TransferCode::interrupted) == lRet) {
      if (mState == RUNNING) {
        WDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::interrupted pStfSenderId={}", pStfSenderId);
      }
      continue;
    } else if (static_cast<std::int64_t>(fair::mq::TransferCode::error) == lRet) {
      EDDLOG_RL(1000, "STF receive failed. what=fair::mq::TransferCode::error err={} errno={} error={}",
        int(lRet), errno, std::string(strerror(errno)));
      continue;
    }

    if (lRet <= 0) {
      WDDLOG_RL(1000, "STF receive failed. size={}", lRet);
      continue;
    }

    // move data to the dedicated region if required
    lStfReceiver.copy_to_region(*lStfData);

    // get Stf ID
    FairMQMessagePtr lHdrMessage = std::move(lStfData->back()); lStfData->pop_back();
    const SubTimeFrame::Header lStfHeader = lStfReceiver.peek_tf_header(lHdrMessage);
    const std::uint64_t lTfId = lStfHeader.mId;

    // WDDLOG("STF received. hdr_size={} num_data={}", lHdrMessage->GetSize(), lStfData->size());

    // signal in flight STF is finished (or error)
    mRpc->recordStfReceived(pStfSenderId, lTfId);

    // send to deserializer thread so that we can keep receiving
    mReceivedDataQueue.push(lTfId, lStfHeader.mOrigin, pStfSenderId, std::move(lHdrMessage), std::move(lStfData));
    lNumStfs++;
  }

  DDDLOG("Exiting input thread [{}]", pFlpIndex);
}

} /* namespace o2::DataDistribution */
