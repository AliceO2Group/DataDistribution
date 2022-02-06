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

namespace o2::DataDistribution
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
  void reset() {
    mReceivedData.flush();
    mStfsForMerging.flush();
    std::unique_lock<std::mutex> lQueueLock(mStfMergerQueueLock);
    mStfMergeMap.clear();
    mMaxMergedTfId = 0;
  }

  void DataHandlerThread(const std::uint32_t pFlpIndex, const std::string pStfSenderId);
  void StfPacingThread();
  void StfDeserializingThread();
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

  /// Deserializing thread
  struct ReceivedStfMeta {
    TimeFrameIdType mStfId;
    SubTimeFrame::Header::Origin mStfOrigin;
    std::chrono::time_point<std::chrono::steady_clock> mTimeReceived;

    FairMQMessagePtr mRecvStfHeaderMeta;
    std::unique_ptr<std::vector<FairMQMessagePtr>> mRecvStfdata;
    std::unique_ptr<SubTimeFrame> mStf;
    std::string mStfSenderId;

    ReceivedStfMeta(const TimeFrameIdType pStfId, const SubTimeFrame::Header::Origin pStfOrigin,
      const std::string &pStfSenderId, FairMQMessagePtr &&pRcvHdrMsg, std::unique_ptr<std::vector<FairMQMessagePtr>> &&pRecvStfdata)
    : mStfId(pStfId),
      mStfOrigin(pStfOrigin),
      mTimeReceived(std::chrono::steady_clock::now()),
      mRecvStfHeaderMeta(std::move(pRcvHdrMsg)),
      mRecvStfdata(std::move(pRecvStfdata)),
      mStf(nullptr),
      mStfSenderId(pStfSenderId)
    { }
  };

  ConcurrentQueue<ReceivedStfMeta> mReceivedData;
  std::thread mStfPacingThread;

  std::mutex mStfMergerQueueLock;
    std::condition_variable mStfMergerCondition;
    std::atomic_bool mStfMergerRun = false;
    std::map<TimeFrameIdType, std::vector<ReceivedStfMeta> > mStfMergeMap;
    std::map<TimeFrameIdType, std::uint64_t> mStfMergeCountMap; // contains number of stfs when all Stfsenders are reached
    std::uint64_t mMaxMergedTfId = 0;

    inline void triggerStfMerger() {
      mStfMergerRun = true;
      mStfMergerCondition.notify_one();
    }

  /// Stf Deserializer (add O2 headers etc)
  void deserialize_headers(std::vector<ReceivedStfMeta> &pStfs); // only the leading split-payload hdr message
  bool is_topo_stf(const std::vector<ReceivedStfMeta> &pStfs) const; // check if topological (S)TF

  std::thread mStfDeserThread;

  /// STF Merger
  ConcurrentQueue<std::vector<ReceivedStfMeta>> mStfsForMerging;
  std::thread mStfMergerThread;

  /// Output pipeline stage
  unsigned mOutStage;
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_BUILDER_INPUT_H_ */
