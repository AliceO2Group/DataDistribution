// Copyright 2019-2022 CERN and copyright holders of ALICE O2.
// See https://alice-o2.web.cern.ch/copyright for details of the copyright holders.
// All rights not expressly granted are reserved.
//
// This software is distributed under the terms of the GNU General Public
// License v3 (GPL Version 3), copied verbatim in the file "COPYING".
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \author Gvozden Nešković, Frankfurt Institute for Advanced Studies and Goethe University Frankfurt

#ifndef ALICEO2_TF_BUILDER_INPUT_H_
#define ALICEO2_TF_BUILDER_INPUT_H_

#include "TfBuilderInputDefs.h"
#include "TfBuilderInputFairMQ.h"
#include "TfBuilderInputUCX.h"

#include <ConfigConsul.h>
#include <discovery.pb.h>

#include <SubTimeFrameDataModel.h>
#include <ConcurrentQueue.h>
#include <MemoryUtils.h>

#include <vector>
#include <map>

#include <condition_variable>
#include <mutex>
#include <thread>

namespace o2::DataDistribution
{

class TfBuilderDevice;
class TfBuilderRpcImpl;

class TfBuilderInput
{
 public:
  TfBuilderInput() = delete;
  TfBuilderInput(TfBuilderDevice& pStfBuilderDev, std::shared_ptr<ConsulTfBuilder> pConfig, std::shared_ptr<TfBuilderRpcImpl> pRpc, unsigned pOutStage);

  bool start();
  bool map_data_region() { return mInputUCX ? mInputUCX->map_data_region() : true; }

  void stop();
  void reset() {
    mReceivedDataQueue->flush();
    mStfsForMerging.flush();
    std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
    mStfMergeMap.clear();
    mMaxMergedTfId = 0;
    mStfIdsToDrop.clear();
  }

  auto getStfRequestQueue() const { return mStfRequestQueue; }
  auto getDataQueue() const { return mReceivedDataQueue; }

  void StfPacingThread();
  void StfDeserializingThread();
  void StfMergerThread();

 private:
  volatile InputRunState mState = CONFIGURING;

  /// Main TimeFrameBuilder O2 device
  TfBuilderDevice& mDevice;

  /// Consul discovery config
  std::shared_ptr<ConsulTfBuilder> mConfig;

  /// RPC service
  std::shared_ptr<TfBuilderRpcImpl> mRpc;

  // Partition info
  std::uint32_t mNumStfSenders = 0;

  /// FairMQ input
  std::unique_ptr<TfBuilderInputFairMQ> mInputFairMQ;

  /// UCX input
  std::unique_ptr<TfBuilderInputUCX> mInputUCX;

  /// Stf Request ID (StfSenderId)
  std::shared_ptr<ConcurrentQueue<std::string> > mStfRequestQueue;

  /// Received Stfs from input stage
  std::shared_ptr<ConcurrentQueue<ReceivedStfMeta> > mReceivedDataQueue;
  std::thread mStfPacingThread;

  /// Stf Deserializer (add O2 headers etc)
  void deserialize_headers(std::vector<ReceivedStfMeta> &pStfs); // only the leading split-payload hdr message
  bool is_topo_stf(const std::vector<ReceivedStfMeta> &pStfs) const; // check if topological (S)TF
  std::thread mStfDeserThread;

  std::mutex mStfMergerQueueLock;
    std::condition_variable mStfMergerCondition;
    std::atomic_bool mStfMergerRun = false;
    std::map<TimeFrameIdType, std::vector<ReceivedStfMeta> > mStfMergeMap;
    std::map<TimeFrameIdType, std::uint64_t> mStfMergeCountMap; // contains number of stfs when all Stfsenders are reached
    std::uint64_t mMaxMergedTfId = 0;
    std::set<std::uint64_t> mStfIdsToDrop;

    inline void triggerStfMerger() {
      mStfMergerRun = true;
      mStfMergerCondition.notify_one();
    }

  /// STF Merger
  ConcurrentQueue<std::vector<ReceivedStfMeta>> mStfsForMerging;
  std::thread mStfMergerThread;

  /// Output pipeline stage
  unsigned mOutStage;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_BUILDER_INPUT_H_ */
