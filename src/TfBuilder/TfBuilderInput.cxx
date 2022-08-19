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

#include <DataDistributionOptions.h>
#include <DataDistMonitoring.h>

#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

TfBuilderInput::TfBuilderInput(TfBuilderDevice& pStfBuilderDev, std::shared_ptr<ConsulTfBuilder> pConfig, std::shared_ptr<TfBuilderRpcImpl> pRpc, unsigned pOutStage)
    : mDevice(pStfBuilderDev),
      mConfig(pConfig),
      mRpc(pRpc),
      mOutStage(pOutStage)
  {
    // initialize request and data queues
    mStfRequestQueue = std::make_shared<ConcurrentQueue<std::string>>();
    mReceivedDataQueue = std::make_shared<ConcurrentQueue<ReceivedStfMeta>>();

    // Select which backend is used
    auto lTransportOpt = mConfig->getStringParam(DataDistNetworkTransportKey, DataDistNetworkTransportDefault);
    if (lTransportOpt == "fmq" || lTransportOpt == "FMQ" || lTransportOpt == "fairmq" || lTransportOpt == "FAIRMQ") {
      mInputFairMQ = std::make_unique<TfBuilderInputFairMQ>(pRpc, pStfBuilderDev.TfBuilderI(), *mStfRequestQueue, *mReceivedDataQueue);
    } else {
      mInputUCX = std::make_unique<TfBuilderInputUCX>(mConfig, pRpc, pStfBuilderDev.TfBuilderI(), *mStfRequestQueue, *mReceivedDataQueue);
    }
  }

bool TfBuilderInput::start()
{
  // make max number of listening channels for the partition
  mDevice.GetConfig()->SetProperty<int>("io-threads", (int) std::min(std::thread::hardware_concurrency(), 32u));
  auto lTransportFactory = fair::mq::TransportFactory::CreateTransportFactory("zeromq", "", mDevice.GetConfig());

  // start the input stage
  if (mInputFairMQ) {
    mInputFairMQ->start(mConfig, lTransportFactory);
  }

  if (mInputUCX) {
    if (!mInputUCX->start()) {
      return false;
    }
  }

  // Start all the threads
  mState = RUNNING;

  // Start the pacer thread
  mReceivedDataQueue->start();
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

  return true;
}

void TfBuilderInput::stop()
{
  // first stop accepting TimeFrames
  mRpc->stopAcceptingTfs();
  mState = TERMINATED;

  // stop FaiMQ input
  if (mInputFairMQ) {
    mInputFairMQ->stop(mConfig);
  }

  // stop UCX input
  if (mInputUCX) {
    mInputUCX->stop();
  }

  //Wait for pacer thread
  mReceivedDataQueue->stop();
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
      triggerStfMerger();
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
  DDDLOG("TfBuilderInput: Teardown complete.");
}

/// TF building pacing thread: rename topo TF ids, and queue physics TFs for merging
void TfBuilderInput::StfPacingThread()
{
  // Deserialization object (stf ID)
  IovDeserializer lStfReceiver(mDevice.TfBuilderI());

  while (mState == RUNNING) {

    auto lData = mReceivedDataQueue->pop_wait_for(500ms);
    if (lData == std::nullopt) {
      continue;
    }
    auto &lStfInfo = lData.value();

    std::uint64_t lTfId = lStfInfo.mStfId;

    if (lStfInfo.mType == ReceivedStfMeta::MetaType::ADD) {
      // only record the intent to build a TF
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
      assert (mStfMergeMap.count(lTfId) == 0);
      assert (mStfMergeMap.empty() || (mStfMergeMap.rbegin()->first < lTfId));

      mStfMergeMap[lTfId].reserve(mNumStfSenders);
      continue;
    } else if (lStfInfo.mType == ReceivedStfMeta::MetaType::DELETE) {
      // remove tf merge intent if no StfSenders were contacted
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
      assert (mStfMergeMap.count(lTfId) == 1);
      assert (mStfMergeMap[lTfId].empty());

      mStfIdsToDrop.insert(lTfId); // set the drop flag for receiving of the future stfs
      mStfMergeMap.erase(lTfId);
      continue;
    }

    assert (lStfInfo.mType == ReceivedStfMeta::MetaType::INFO);
    assert (lStfInfo.mRecvStfData);

    // Rename STF id if this is a Topological TF
    if (lStfInfo.mStfOrigin == SubTimeFrame::Header::Origin::eReadoutTopology) {
      // deserialize here to be able to rename the stf
      lStfInfo.mStf = std::move(lStfReceiver.deserialize(*lStfInfo.mRecvStfHeaderMeta.get(), *lStfInfo.mRecvStfData));
      lStfInfo.mRecvStfData = nullptr;

      const std::uint64_t lNewTfId = mRpc->getIdForTopoTf(lStfInfo.mStfSenderId, lStfInfo.mStfId);

      DDDLOG_RL(5000, "Deserialized STF. stf_id={} new_id={}", lStfInfo.mStf->id(), lNewTfId);
      lStfInfo.mStf->updateId(lNewTfId);
      lTfId = lNewTfId;
      lStfInfo.mStfId = lNewTfId;
    }

    // TfScheduler should manage memory of the region and not overcommit the TfBuilders
    { // Push the STF into the merger queue
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);

      // check if there were issues with requesting STFs
      if (mStfIdsToDrop.count(lTfId) > 0) {
        mStfMergeMap.erase(lTfId);
        continue;
      }

      if (lTfId > mMaxMergedTfId) {
        mStfMergeMap[lTfId].push_back(std::move(lStfInfo));
      } else {
        EDDLOG_RL(1000, "StfPacingThread: Received STF ID is smaller than the last built STF. stfs_id={} stf_id={} last_stf_id={}",
          lStfInfo.mStfSenderId, lTfId, mMaxMergedTfId);

        // reordered or duplicated STF ? Cleanup the merge map.
        mStfMergeMap.erase(lTfId);
      }
    }
    // wake up the merging thread
    triggerStfMerger();
  }

  DDDLOG("Exiting StfPacingThread.");
}


void TfBuilderInput::deserialize_headers(std::vector<ReceivedStfMeta> &pStfs)
{
  // Deserialization object
  IovDeserializer lStfReceiver(mDevice.TfBuilderI());

  for (auto &lStfInfo : pStfs) {
    if (lStfInfo.mStf) {
      continue; // already deserialized
    }

    // deserialize the data
    lStfInfo.mStf = std::move(lStfReceiver.deserialize(*lStfInfo.mRecvStfHeaderMeta.get(), *lStfInfo.mRecvStfData));
    lStfInfo.mRecvStfData = nullptr;
  }
}

/// FMQ->STF thread
/// This thread can block waiting on free O2 Header memory
void TfBuilderInput::StfDeserializingThread()
{
  while (mState == RUNNING) {

    std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
    mStfMergerCondition.wait_for(lQueueLock, 5ms, [this]{ return mStfMergerRun.load(); });
    mStfMergerRun = false;

    if (mStfMergeMap.empty()) {
      continue; // re-evaluate conditions
    }

    //  Try to complete TF with the smallest ID
    auto lMinTfInfo = mStfMergeMap.begin();
    const auto lStfId = lMinTfInfo->first;
    auto &lStfVector = lMinTfInfo->second;

    // check if the TF should be dropped
    if (mStfIdsToDrop.count(lStfId) > 0) {
      mStfMergeMap.erase(mStfMergeMap.begin());
      continue;
    }

    // deserialize headers of any new STFs
    deserialize_headers(lStfVector);

    // Check the number of expected STFs
    const auto lNumStfsOpt = mRpc->getNumberOfStfs(lStfId);
    if (lNumStfsOpt == std::nullopt) {
      // STF request thread has not finished requesting all STFs yet
      triggerStfMerger();
      continue;
    }

    // Check if the TF is completed
    if (!lStfVector.empty() && (lStfVector.size() == lNumStfsOpt.value())) {
      mStfsForMerging.push(std::move(lStfVector));

      mStfMergeMap.erase(lStfId);
      mStfMergeCountMap.erase(lStfId);
      mRpc->setNumberOfStfs(lStfId, std::nullopt);
    }

    // TODO: add timeout for non-completed TFs?
  }

  IDDLOG("Exiting stf deserializer thread.");
}

/// STF->TF Merger thread
void TfBuilderInput::StfMergerThread()
{
  using namespace std::chrono_literals;

  std::uint64_t lNumBuiltTfs = 0;

  DDMON_RATE("tfbuilder", "tf_input", 0.0);

  while (mState == RUNNING) {

    auto lStfVectorOpt = mStfsForMerging.pop_wait_for(500ms);
    if (lStfVectorOpt == std::nullopt) {
      continue;
    }
    auto &lStfVector = lStfVectorOpt.value();

    assert (lStfVector.size() > 0);

    // merge the current TF!
    const double lBuildDurationMs = since<std::chrono::milliseconds>(
      lStfVector.cbegin()->mTimeReceived, lStfVector.crbegin()->mTimeReceived);

    // start from the first element (using it as the seed for the TF)
    std::unique_ptr<SubTimeFrame> lTf = std::move(lStfVector.begin()->mStf);
    if (!lTf) {
      EDDLOG("StfMergerThread: First Stf is null. (not deserialized?)");
      continue;
    }

    // Add the rest of STFs
    for (auto lStfIter = std::next(lStfVector.begin()); lStfIter != lStfVector.end(); ++lStfIter) {
      lTf->mergeStf(std::move(lStfIter->mStf), lStfIter->mStfSenderId);
    }
    lNumBuiltTfs++;

    const auto lTfId = lTf->id();

    { // Push the STF into the merger queue
      std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
      mMaxMergedTfId = std::max(mMaxMergedTfId, lTfId);
    }

    // account the size of received TF
    mRpc->recordTfBuilt(*lTf);

    DDDLOG_RL(5000, "Building of TF completed. tf_id={} duration_ms={:.4} total_tf={}", lTfId, lBuildDurationMs, lNumBuiltTfs);
    DDMON("tfbuilder", "merge.receive_span_ms", lBuildDurationMs);

    // Queue out the TF for consumption
    DDMON_RATE("tfbuilder", "tf_input", lTf->getDataSize());
    mDevice.queue(mOutStage, std::move(lTf));
  }

  IDDLOG("Exiting STF merger thread.");
}

} /* namespace o2::DataDistribution */
