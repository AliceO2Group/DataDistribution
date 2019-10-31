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
#include <ConcurrentQueue.h>

#include <vector>
#include <map>
#include <thread>

namespace o2
{
namespace DataDistribution
{

class StfSenderDevice;

class StfSenderOutput
{
 public:
  StfSenderOutput() = delete;
  StfSenderOutput(StfSenderDevice& pStfSenderDev)
    : mDevice(pStfSenderDev)
  {
  }

  void start(std::shared_ptr<ConsulStfSender> pDiscoveryConfig);
  void stop();

  bool running() const;

  void StfSchedulerThread();
  void DataHandlerThread(const std::string pTfBuilderId);

  /// RPC requests
  enum ConnectStatus { eOK, eEXISTS, eCONNERR };
  ConnectStatus connectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);
  bool disconnectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);

  void sendStfToTfBuilder(const std::uint64_t pStfId, const std::string &pTfBuilderId, StfDataResponse &pRes);

 private:
  /// Ref to the main SubTimeBuilder O2 device
  StfSenderDevice& mDevice;

  /// Discovery configuration
  std::shared_ptr<ConsulStfSender> mDiscoveryConfig;

  /// Scheduler threads
  std::thread mSchedulerThread;
  std::mutex mScheduledStfMapLock;
  std::map<std::uint64_t, std::unique_ptr<SubTimeFrame>> mScheduledStfMap;

  /// Threads for output channels (to EPNs)
  struct OutputChannelObjects {
    std::string mTfBuilderEndpoint;
    std::unique_ptr<FairMQChannel> mChannel;
    std::unique_ptr<ConcurrentFifo<std::unique_ptr<SubTimeFrame>>> mStfQueue;
    std::thread mThread;

    std::unique_ptr<std::atomic_bool> mRunning;
  };
  mutable std::mutex mOutputMapLock;
  std::map<std::string, OutputChannelObjects> mOutputMap;



};
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STF_SENDER_OUTPUT_H_ */
