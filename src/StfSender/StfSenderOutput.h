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

#include <ConfigConsul.h>

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
  struct StdSenderOutputCounters {
    // buffer state
    struct alignas(128) {
      std::uint64_t mSize = 0;
      std::uint32_t mCnt = 0;
    } mBuffered;
    // buffered in sending
    struct alignas(128) {
      std::uint64_t mSize = 0;
      std::uint32_t mCnt = 0;
    } mInSending;

    // total sent
    struct alignas(128) {
      std::uint64_t mSize = 0;
      std::uint64_t mCnt = 0;
    } mTotalSent;
  };

  StfSenderOutput() = delete;
  StfSenderOutput(StfSenderDevice &pStfSenderDev, stf_pipeline &pPipelineI)
    : mDevice(pStfSenderDev),
      mPipelineI(pPipelineI)
  {
  }

  void start(std::shared_ptr<ConsulStfSender> pDiscoveryConfig);
  void stop();

  bool running() const;

  void StfSchedulerThread();
  void StfDropThread();
  void DataHandlerThread(const std::string pTfBuilderId);
  void StfMonitoringThread();

  /// RPC requests
  enum ConnectStatus { eOK, eEXISTS, eCONNERR };
  ConnectStatus connectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);
  bool disconnectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);

  void sendStfToTfBuilder(const std::uint64_t pStfId, const std::string &pTfBuilderId, StfDataResponse &pRes);

  StdSenderOutputCounters getCounters() {
    std::scoped_lock lLock(mCountersLock);
    return mCounters;
  }

  StdSenderOutputCounters resetCounters() {
    std::scoped_lock lLock(mCountersLock);
    StdSenderOutputCounters lRet = mCounters;
    mCounters = StdSenderOutputCounters();
    return lRet;
  }

 private:
  /// Ref to the main SubTimeBuilder O2 device
  StfSenderDevice& mDevice;
  stf_pipeline &mPipelineI;

  /// Running flag
  std::atomic_bool mRunning = false;
  std::thread mMonitoringThread;

  /// Discovery configuration
  std::shared_ptr<ConsulStfSender> mDiscoveryConfig;
  std::shared_ptr<FairMQTransportFactory>  mZMQTransportFactory;

  /// Scheduler threads
  std::thread mSchedulerThread;
  std::mutex mScheduledStfMapLock;
    std::map<std::uint64_t, std::unique_ptr<SubTimeFrame>> mScheduledStfMap;

  /// Buffer utilization counters
  std::mutex mCountersLock;
    StdSenderOutputCounters mCounters;

  /// Threads for output channels (to EPNs)
  struct OutputChannelObjects {
    std::string mTfBuilderEndpoint;
    std::unique_ptr<FairMQChannel> mChannel;
    std::unique_ptr<CoalescedHdrDataSerializer> mStfSerializer;
    std::unique_ptr<ConcurrentFifo<std::unique_ptr<SubTimeFrame>>> mStfQueue;
    std::thread mThread;
  };

  mutable std::mutex mOutputMapLock;
    std::map<std::string, OutputChannelObjects> mOutputMap;

  // Buffer maintenance
  std::uint64_t mBufferSize = std::uint64_t(32) << 30;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> mDropQueue;
  std::thread mStfDropThread;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STF_SENDER_OUTPUT_H_ */
