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
  StfSenderOutput(StfSenderDevice &pStfSenderDev, stf_pipeline &pPipelineI)
    : mDevice(pStfSenderDev),
      mPipelineI(pPipelineI)
  {
  }

  void start(std::shared_ptr<ConsulStfSender> pDiscoveryConfig);
  void start_standalone(std::shared_ptr<ConsulStfSender> pDiscoveryConfig);
  void stop();

  bool running() const;

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
  std::atomic_uint64_t mKeepTarget = 512ULL << 20;
  std::atomic_uint64_t mDeletePercentage = 50;
  std::mutex mStfKeepMapLock;
    std::map<std::uint64_t, std::unique_ptr<SubTimeFrame>> mStfKeepMap;
  std::thread mStfKeepThread;

  /// Discovery configuration
  std::shared_ptr<ConsulStfSender> mDiscoveryConfig;
  std::shared_ptr<fair::mq::TransportFactory> mZMQTransportFactory;

  /// Scheduler threads
  std::thread mSchedulerThread;
  std::uint64_t mLastStfId = 0;
  std::mutex mScheduledStfMapLock;
    std::map<std::uint64_t, ScheduledStfInfo> mScheduledStfMap;

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
