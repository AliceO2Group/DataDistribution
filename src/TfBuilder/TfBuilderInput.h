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

#ifndef ALICEO2_TF_BUILDER_INPUT_H_
#define ALICEO2_TF_BUILDER_INPUT_H_

#include <ConfigConsul.h>
#include <discovery.pb.h>

#include <SubTimeFrameDataModel.h>
#include <ConcurrentQueue.h>

#include <vector>
#include <map>

#include <condition_variable>
#include <mutex>
#include <thread>

namespace o2
{
namespace DataDistribution
{

class TfBuilderDevice;
class TfBuilderRpcImpl;

class TfBuilderInput
{
 public:
  TfBuilderInput() = delete;
  TfBuilderInput(TfBuilderDevice& pStfBuilderDev, std::shared_ptr<TfBuilderRpcImpl> pRpc, unsigned pOutStage)
    : mDevice(pStfBuilderDev),
      mRpc(pRpc),
      mOutStage(pOutStage)
  {
  }

  bool start(std::shared_ptr<ConsulTfBuilder> pConfig);
  void stop(std::shared_ptr<ConsulTfBuilder> pConfig);

  void DataHandlerThread(const std::uint32_t pFlpIndex);
  void StfMergerThread();

 private:
  enum RunState { CONFIGURING, RUNNING, TERMINATED };
  volatile RunState mState = CONFIGURING;

  /// Main TimeFrameBuilder O2 device
  TfBuilderDevice& mDevice;

  /// RPC service
  std::shared_ptr<TfBuilderRpcImpl> mRpc;

  // Partition info
  std::uint32_t mNumStfSenders = 0;

  /// StfBuilder channels
  std::vector<std::unique_ptr<FairMQChannel>> mStfSenderChannels;

  /// Threads for input channels (per FLP)
  std::map<std::string, std::thread> mInputThreads;

  /// STF Merger
  std::thread mStfMergerThread;
  std::mutex mStfMergerQueueLock;
  std::condition_variable mStfMergerCondition;

  struct ReceivedStfMeta {
    std::chrono::time_point<std::chrono::system_clock> mTimeReceived;
    std::unique_ptr<SubTimeFrame> mStf;

    ReceivedStfMeta(std::unique_ptr<SubTimeFrame>&& pStf)
    : mTimeReceived(std::chrono::system_clock::now()),
      mStf(std::move(pStf))
    {}
  };

  std::map<TimeFrameIdType, std::vector<ReceivedStfMeta>> mStfMergeMap;
  std::uint64_t mStfCount = 0;

  /// Output pipeline stage
  unsigned mOutStage;
};
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_BUILDER_INPUT_H_ */
