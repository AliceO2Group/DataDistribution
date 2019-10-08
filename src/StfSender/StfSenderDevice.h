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

#ifndef ALICEO2_STF_SENDER_DEVICE_H_
#define ALICEO2_STF_SENDER_DEVICE_H_

#include "StfSenderRpc.h"
#include "StfSenderOutput.h"

#include <ConfigConsul.h>
#include <TfSchedulerRpcClient.h>

#include <SubTimeFrameFileSink.h>
#include <Utilities.h>
#include <RootGui.h>

#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>

namespace o2
{
namespace DataDistribution
{

class ConsulConfig;

enum StfSenderPipeline {
  eReceiverOut = 0,

  eFileSinkIn = 0,
  eFileSinkOut = 1,

  eSenderIn = 1,

  eNullIn = 2, // delete/drop
  ePipelineSize = 2,
  eInvalidStage = -1,
};

class StfSenderDevice : public DataDistDevice,
                        public IFifoPipeline<std::unique_ptr<SubTimeFrame>>
{
 public:
  static constexpr const char* OptionKeyInputChannelName = "input-channel-name";
  static constexpr const char* OptionKeyStandalone = "stand-alone";
  static constexpr const char* OptionKeyMaxBufferedStfs = "max-buffered-stfs";
  static constexpr const char* OptionKeyMaxConcurrentSends = "max-concurrent-sends";
  static constexpr const char* OptionKeyGui = "gui";

  /// Default constructor
  StfSenderDevice();

  /// Default destructor
  ~StfSenderDevice() override;

  void InitTask() final;
  void ResetTask() final;

  std::int64_t stfCountIncFetch() { return ++mNumStfs; }
  std::int64_t stfCountDecFetch() { return --mNumStfs; }
  std::int64_t stfCountFetch() const { return mNumStfs; }

  bool standalone() const { return mStandalone; }

  TfSchedulerRpcClient& TfSchedRpcCli() { return mTfSchedulerRpcClient; }


  bool guiEnabled() const noexcept { return mBuildHistograms; }

 protected:
  void PreRun() final;
  void PostRun() final {};
  bool ConditionalRun() final;

  void StfReceiverThread();

  unsigned getNextPipelineStage(unsigned pStage) final
  {
    StfSenderPipeline lNextStage = eInvalidStage;

    assert(mNumStfs >= 0);

    switch (pStage) {
      case eReceiverOut: {
        const auto lNumStfs = stfCountIncFetch();
        if (mPipelineLimit && (lNumStfs > mMaxStfsInPipeline)) {
          stfCountDecFetch();
          lNextStage = eNullIn;
        } else {
          lNextStage = mFileSink.enabled() ? eFileSinkIn : eSenderIn;
        }
        break;
      }
      case eFileSinkOut:
        lNextStage = eSenderIn;
        break;
      default:
        throw std::runtime_error("pipeline error");
    }

    assert(lNextStage >= eFileSinkIn && lNextStage <= eSenderIn);
    return lNextStage;
  }

  /// Configuration
  std::string mInputChannelName;
  bool mStandalone;
  std::int64_t mMaxStfsInPipeline;
  std::uint32_t mMaxConcurrentSends;
  bool mPipelineLimit;

  /// Discovery configuration
  std::shared_ptr<ConsulStfSender> mDiscoveryConfig;

  /// Scheculer RPC client
  TfSchedulerRpcClient mTfSchedulerRpcClient;

  /// Receiver threads
  std::thread mReceiverThread;

  /// File sink
  SubTimeFrameFileSink mFileSink;

  /// Output stage handler
  StfSenderOutput mOutputHandler;

  /// RPC service
  StfSenderRpcImpl mRpcServer;

  /// number of STFs in the process
  std::atomic_int64_t mNumStfs{ 0 };

  /// Root GUI stuff
  void GuiThread();
  bool mBuildHistograms = true;
  std::unique_ptr<RootGui> mGui;
  std::thread mGuiThread;
};
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STF_SENDER_DEVICE_H_ */
