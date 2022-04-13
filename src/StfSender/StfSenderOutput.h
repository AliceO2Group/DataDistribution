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

#ifndef ALICEO2_STF_SENDER_OUTPUT_H_
#define ALICEO2_STF_SENDER_OUTPUT_H_

#include "StfSenderOutputDefs.h"
#include "StfSenderOutputFairMQ.h"
#include "StfSenderOutputUCX.h"

#include <ConfigConsul.h>
#include <DataDistributionOptions.h>

#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameVisitors.h>
#include <ConcurrentQueue.h>

#include <vector>
#include <map>
#include <thread>

namespace o2::DataDistribution
{

class StfSenderDevice;
class CoalescedHdrDataSerializer;

class StfSenderOutput
{
  using stf_pipeline = IFifoPipeline<std::unique_ptr<SubTimeFrame>>;

public:
  StfSenderOutput() = delete;
  StfSenderOutput(StfSenderDevice &pStfSenderDev, stf_pipeline &pPipelineI, std::shared_ptr<SubTimeFrameCopyBuilder> pStfCopyBuilder)
    : mDevice(pStfSenderDev),
      mPipelineI(pPipelineI),
      mStfCopyBuilder(pStfCopyBuilder)
  {
  }

  void start(std::shared_ptr<ConsulStfSender> pDiscoveryConfig);
  void register_regions();
  void start_standalone(std::shared_ptr<ConsulStfSender> pDiscoveryConfig);
  void stop();

  bool running() const;

  void StfOrderThread();
  void StfCopyAllocThread();
  void StfCopyThread();
  void StfSchedulerThread();
  void StfKeepThread();
  void StfDropThread();
  void StaleStfThread();
  void StfMonitoringThread();

  /// RPC requests
  // FairMQ
  ConnectStatus connectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);
  bool disconnectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);
  // UCX
  ConnectStatus connectTfBuilderUCX(const std::string &pTfBuilderId, const std::string &pIp, unsigned pPort);
  bool disconnectTfBuilderUCX(const std::string &pTfBuilderId);
  // Data
  void sendStfToTfBuilder(const std::uint64_t pStfId, const std::string &pTfBuilderId, StfDataResponse &pRes);

  /// Counters
  StdSenderOutputCounters::Values getCounters() {
    std::scoped_lock lLock(mCounters.mCountersLock);
    return mCounters.mValues;
  }
  void resetCounters() {

    { // clear the state
      std::scoped_lock lMapLock(mStfKeepMapLock);
      mStfKeepMap.clear();
    }

    { // clear the buffers
      std::scoped_lock lLock(mCounters.mCountersLock);
      mCounters.mValues = StdSenderOutputCounters::Values();
      mLastStfId = 0;
    }

    { // clear the stf maps
      std::scoped_lock lLock(mScheduledStfMapLock);
      mScheduledStfMap.clear();
      mSchedulingResult.clear();
    }

    { // clear the standalone keep map
      std::scoped_lock lMapLock(mStfKeepMapLock);
      mStfKeepMap.clear();
    }
  }

 private:
  /// Ref to the main SubTimeBuilder O2 device
  StfSenderDevice& mDevice;
  stf_pipeline &mPipelineI;

  /// Running flag
  std::atomic_bool mRunning = false;

  /// Monitoring thread
  std::thread mMonitoringThread;

  /// Stale STF thread
  std::thread mStaleStfThread;
  std::uint64_t mStaleStfTimeoutMs = StaleStfTimeoutMsDefault;

  /// Stf keeper thread for standalone runs
  std::atomic_uint64_t mKeepTarget = StandaloneStfDataBufferSizeMBDefault << 20;
  std::atomic_uint64_t mDeletePercentage = StandaloneStfDeleteChanceDefault;
  std::mutex mStfKeepMapLock;
    std::map<std::uint64_t, std::unique_ptr<SubTimeFrame>> mStfKeepMap;
  std::thread mStfKeepThread;

  /// Discovery configuration
  std::shared_ptr<ConsulStfSender> mDiscoveryConfig;
  std::shared_ptr<fair::mq::TransportFactory> mZMQTransportFactory;

  /// StfCopy builder; not used if nullptr
  std::shared_ptr<SubTimeFrameCopyBuilder> mStfCopyBuilder;

  /// Stf copy threads and queues
  struct StfCopyInfo {
    std::unique_ptr<SubTimeFrame> mStf;
    std::vector<void*> mLinkBuffers;
  };

  std::thread mStfOrderThread;
  std::thread mCopyAllocThread;
  std::vector<std::thread> mCopyThreads;

  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> mCopyAllocQueue;
  ConcurrentFifo<StfCopyInfo> mCopyQueue;

  std::mutex mStfOrderingLock;
  std::condition_variable mStfOrderingCv;
  std::queue<std::uint64_t> mStfOrderingQueue;

  /// Scheduler threads
  struct StfSchedInfo {
    std::unique_ptr<SubTimeFrame> mStf;
    bool mMemoryPressure = false;
  };
  ConcurrentFifo<StfSchedInfo> mScheduleQueue;
  std::thread mSchedulerThread;
  std::uint64_t mLastStfId = 0;
  std::mutex mScheduledStfMapLock;
    std::map<std::uint64_t, ScheduledStfInfo> mScheduledStfMap;
    std::map<std::string, std::pair<std::uint64_t, StfDataResponse::StfDataStatus> > mSchedulingResult; // use for answering retried StfRequest() grpc

  /// Buffer utilization counters
  StdSenderOutputCounters mCounters;

  /// Buffer maintenance
  std::uint64_t mBufferSize = std::uint64_t(32) << 30;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> mDropQueue;
  std::thread mStfDropThread;

  /// FairMQ output
  std::unique_ptr<StfSenderOutputFairMQ> mOutputFairMQ;
  /// UCX
  std::unique_ptr<StfSenderOutputUCX> mOutputUCX;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STF_SENDER_OUTPUT_H_ */
