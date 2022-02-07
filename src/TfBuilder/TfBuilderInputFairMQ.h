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

#ifndef TF_BUILDER_INPUT_FAIRMQ_H_
#define TF_BUILDER_INPUT_FAIRMQ_H_

#include "TfBuilderInputDefs.h"

#include <ConfigConsul.h>
#include <discovery.pb.h>

#include <SubTimeFrameDataModel.h>
#include <ConcurrentQueue.h>

#include <vector>
#include <map>

#include <thread>

namespace o2::DataDistribution
{

class TimeFrameBuilder;
class TfBuilderRpcImpl;

class TfBuilderInputFairMQ
{
 public:
  TfBuilderInputFairMQ() = delete;
  TfBuilderInputFairMQ(std::shared_ptr<TfBuilderRpcImpl> pRpc, TimeFrameBuilder &pTfBldr, ConcurrentQueue<ReceivedStfMeta> &pStfMetaQueue)
    : mRpc(pRpc),
      mTimeFrameBuilder(pTfBldr),
      mReceivedDataQueue(pStfMetaQueue)
  {
  }

  bool start(std::shared_ptr<ConsulTfBuilder> pConfig, std::shared_ptr<FairMQTransportFactory> pZMQTransportFactory);
  void stop(std::shared_ptr<ConsulTfBuilder> pConfig);
  void reset() {

  }

  void DataHandlerThread(const std::uint32_t pFlpIndex, const std::string pStfSenderId);

 private:
  std::atomic<InputRunState> mState = CONFIGURING;

  /// RPC service
  std::shared_ptr<TfBuilderRpcImpl> mRpc;

  /// TimeFrameBuilder
  TimeFrameBuilder &mTimeFrameBuilder;

  // Partition info
  std::uint32_t mNumStfSenders = 0;

  /// StfBuilder channels
  std::vector<std::unique_ptr<FairMQChannel>> mStfSenderChannels;

  /// Threads for input channels (per FLP)
  std::map<std::string, std::thread> mInputThreads;

  /// Queue for received STFs
  ConcurrentQueue<ReceivedStfMeta> &mReceivedDataQueue;
};

} /* namespace o2::DataDistribution */

#endif /* TF_BUILDER_INPUT_FAIRMQ_H_ */
