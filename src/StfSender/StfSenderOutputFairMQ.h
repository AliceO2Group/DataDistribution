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

#ifndef STF_SENDER_OUTPUT_FMQ_H_
#define STF_SENDER_OUTPUT_FMQ_H_

#include "StfSenderOutputDefs.h"

#include <ConfigConsul.h>
#include <ConcurrentQueue.h>

#include <SubTimeFrameVisitors.h>

#include <vector>
#include <map>
#include <thread>

namespace o2::DataDistribution
{

class StfSenderOutputFairMQ {
public:

  StfSenderOutputFairMQ(std::shared_ptr<ConsulStfSender> pDiscoveryConfig, StdSenderOutputCounters &pCounters)
  : mDiscoveryConfig(pDiscoveryConfig),
    mCounters(pCounters)
  {

  }

  void start(std::shared_ptr<FairMQTransportFactory> pZMQTransportFactory);
  void stop();

  /// RPC requests
  ConnectStatus connectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);
  bool disconnectTfBuilder(const std::string &pTfBuilderId, const std::string &lEndpoint);

  bool sendStfToTfBuilder(const std::string &pTfBuilderId, std::unique_ptr<SubTimeFrame> &&pStf);

  void DataHandlerThread(const std::string pTfBuilderId);

private:

  /// Running flag
  std::atomic_bool mRunning = false;

  /// Discovery configuration
  std::shared_ptr<ConsulStfSender> mDiscoveryConfig;
  std::shared_ptr<FairMQTransportFactory>  mZMQTransportFactory;

  // Global stf counters
  StdSenderOutputCounters &mCounters;

  /// Threads for output channels (to EPNs)
  struct OutputChannelObjects {
    std::string mTfBuilderEndpoint;
    std::unique_ptr<FairMQChannel> mChannel;
    std::unique_ptr<IovSerializer> mStfSerializer;
    std::unique_ptr<ConcurrentFifo<std::unique_ptr<SubTimeFrame>>> mStfQueue;
    std::thread mThread;
  };

  mutable std::mutex mOutputMapLock;
    std::map<std::string, OutputChannelObjects> mOutputMap;
};

} /* namespace o2::DataDistribution */

#endif /* STF_SENDER_OUTPUT_FMQ_H_ */
