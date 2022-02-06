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

#include "StfSenderOutputFairmq.h"

#include <DataDistLogger.h>
#include <DataDistMonitoring.h>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

void StfSenderOutputFairmq::start(std::shared_ptr<FairMQTransportFactory> pZMQTransportFactory)
{
  mZMQTransportFactory = pZMQTransportFactory;

  mRunning = true;
}


void StfSenderOutputFairmq::stop()
{
  mRunning = false;
  {
    std::scoped_lock lLock(mOutputMapLock);

    for (auto& lIdOutputIt : mOutputMap) {
      lIdOutputIt.second.mStfQueue->stop();
    }

    // wait for threads to exit
    for (auto& lIdOutputIt : mOutputMap) {
      if (lIdOutputIt.second.mThread.joinable()) {
        lIdOutputIt.second.mThread.join();
      }
    }

    mOutputMap.clear();
  }
}

ConnectStatus StfSenderOutputFairmq::connectTfBuilder(const std::string &pTfBuilderId, const std::string &pEndpoint)
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
        create_thread_member(lThreadName, &StfSenderOutputFairmq::DataHandlerThread, this, pTfBuilderId)
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

bool StfSenderOutputFairmq::disconnectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint)
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

bool StfSenderOutputFairmq::sendStfToTfBuilder(const std::string &pTfBuilderId, std::unique_ptr<SubTimeFrame> &&pStf) {
  std::scoped_lock lLock(mOutputMapLock);

  auto lTfBuilderIter = mOutputMap.find(pTfBuilderId);

  if (lTfBuilderIter == mOutputMap.end()) {
    EDDLOG_GRL(1000, "OutputFairmq::sendStfToTfBuilder: TfBuilder not known to StfSender. tfb_id={}", pTfBuilderId);
    return false;
  }

  lTfBuilderIter->second.mStfQueue->push(std::move(pStf));

  if (lTfBuilderIter->second.mStfQueue->size() > 50) {
    WDDLOG_RL(1000, "SendToTfBuilder: STF queue backlog. queue_size={}", lTfBuilderIter->second.mStfQueue->size());
  }

  return true;
}


/// Sending thread
void StfSenderOutputFairmq::DataHandlerThread(const std::string pTfBuilderId)
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
      if (mRunning.load()){
        EDDLOG("StfSenderOutput[{}]: exception on send, what={}", pTfBuilderId, e.what());
      } else {
        IDDLOG("StfSenderOutput[{}](NOT RUNNING): exception on send. what={}", pTfBuilderId, e.what());
      }
      break;
    }

    // update buffer status
    lNumSentStfs += 1;

    StdSenderOutputCounters::Values lCounters;
    {
      std::scoped_lock lCntLock(mCounters.mCountersLock);
      mCounters.mValues.mBuffered.mSize -= lStfSize;
      mCounters.mValues.mBuffered.mCnt -= 1;
      mCounters.mValues.mInSending.mSize -= lStfSize;
      mCounters.mValues.mInSending.mCnt -= 1;
      mCounters.mValues.mTotalSent.mSize += lStfSize;
      mCounters.mValues.mTotalSent.mCnt += 1;

      lCounters = mCounters.mValues;
    }

    if (lCounters.mInSending.mCnt > 100) {
      DDDLOG_RL(2000, "DataHandlerThread: Number of buffered STFs. tfb_id={} num_stfs={} num_stf_total={} size_stf_total={}",
        pTfBuilderId, lInputStfQueue->size(), lCounters.mInSending.mCnt, lCounters.mInSending.mSize);
    }

    DDMON("stfsender", "stf_output.stf_id", lStfId);
  }

  DDDLOG("Exiting StfSenderOutput[{}]", pTfBuilderId);
}


} /* o2::DataDistribution */