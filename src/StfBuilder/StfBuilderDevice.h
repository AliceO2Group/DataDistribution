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

#ifndef ALICEO2_STFBUILDER_DEVICE_H_
#define ALICEO2_STFBUILDER_DEVICE_H_

#include "StfBuilderInput.h"

#include <ReadoutDataModel.h>
#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameFileSink.h>
#include <SubTimeFrameFileSource.h>
#include <ConcurrentQueue.h>
#include <Utilities.h>

#include <deque>
#include <memory>
#include <mutex>
#include <condition_variable>

#include <stdexcept>

#include <DataDistLogger.h>

namespace o2
{
namespace DataDistribution
{

enum StfBuilderPipeline {
  // only input stages
  eStfBuilderOut = 0,
  eStfFileSourceOut = 0,

  // input/output stages
  eStfFileSinkIn = 0,
  eStfFileSinkOut = 1,

  // output only stages
  eStfSendIn = 1,

  eStfNullIn = 2, // delete/drop
  eStfPipelineSize = 2,
  eStfInvalidStage = -1,
};

class StfBuilderDevice : public DataDistDevice,
                         public IFifoPipeline<std::unique_ptr<SubTimeFrame>>
{
 public:
  constexpr static int gStfOutputChanId = 0;

  static constexpr const char* OptionKeyInputChannelName = "input-channel-name";
  static constexpr const char* OptionKeyOutputChannelName = "output-channel-name";
  static constexpr const char* OptionKeyDplChannelName = "dpl-channel-name";
  static constexpr const char* OptionKeyStandalone = "stand-alone";
  static constexpr const char* OptionKeyMaxBufferedStfs = "max-buffered-stfs";
  static constexpr const char* OptionKeyMaxBuiltStfs = "max-built-stfs";

  static constexpr const char* OptionKeyStfDetector = "detector";
  static constexpr const char* OptionKeyRhdVer = "detector-rdh";
  static constexpr const char* OptionKeySubSpec = "detector-subspec";

  static constexpr const char* OptionKeyRdhSanityCheck = "rdh-data-check";
  static constexpr const char* OptionKeyFilterEmptyTriggerData = "rdh-filter-empty-trigger";

  static bpo::options_description getDetectorProgramOptions();
  static bpo::options_description getStfBuildingProgramOptions();
  static o2::header::DataOrigin getDataOriginFromOption(const std::string pArg);

  /// Default constructor
  StfBuilderDevice();

  /// Default destructor
  ~StfBuilderDevice() override;

  bool dplEnabled() const noexcept { return I().mDplEnabled; }
  bool isStandalone() const noexcept { return I().mStandalone; }

  const std::string& getInputChannelName() const { return I().mInputChannelName; }
  const std::string& getDplChannelName() const { return I().mDplChannelName; }

  auto& getOutputChannel() {
    if (dplEnabled()) {
      return this->GetChannel(I().mDplChannelName);
    }

    return this->GetChannel(I().mOutputChannelName);
  }

 protected:
  virtual void Init() override final;
  virtual void Reset() override final;
  virtual void InitTask() override final;
  virtual void ResetTask() override final;
  virtual bool ConditionalRun() override final;

  // stop/restart file source
  virtual void PreRun() override final {
    I().mPaused = false;
    if (I().mFileSource) {
      I().mFileSource->resume();
      DDLOGF(fair::Severity::INFO, "Restarting file source.");
    }
    I().mRestartRateCounter = true;
  }
  virtual void PostRun() override final {
    I().mPaused = true;
    if (I().mFileSource) {
      I().mFileSource->pause();
      DDLOGF(fair::Severity::INFO, "Pausing file source.");
    }
  }

  bool tryPopOldestStfs()
  {
    // try to drop one STF starting from back-end queues
    if (this->try_pop(eStfSendIn)) {
      return true;
    }

    if (this->try_pop(eStfFileSinkIn)) {
      return true;
    }

    return false;
  }

  unsigned getNextPipelineStage(unsigned pStage) final
  {
    StfBuilderPipeline lNextStage = eStfInvalidStage;

    switch (pStage) {
      case eStfBuilderOut:
      /* case eStfFileSourceOut: */
      {
        I().mNumStfs++;

        if (I().mPipelineLimit && (I().mNumStfs >= I().mMaxStfsInPipeline)) {

          // DROP policy in StfBuilder is to keep most current STFs. This will ensure that all
          // StfBuilders have the same set of STFs ready for distribution

          DDLOGF_RL(500, fair::Severity::WARNING, "Dropping oldest STF due to reaching the maximum number of buffered "
            "STFs in the process ({}). Consider increasing the limit, or reducing the input data rate.",
            I().mMaxStfsInPipeline);

          if (tryPopOldestStfs()) {
            I().mNumStfs--;
          }
        }

        if (I().mFileSink->enabled()) {
          I().mNumStfs--;
          lNextStage = eStfFileSinkIn;
        } else {
          lNextStage = eStfSendIn;
        }
        break;
      }
      case eStfFileSinkOut:
      {
        I().mNumStfs++;
        lNextStage = eStfSendIn;
        break;
      }
      default:
        throw std::runtime_error("pipeline error");
    }

    if (!(lNextStage >= eStfFileSinkIn && lNextStage <= eStfNullIn)) {
      DDLOGF(fair::Severity::ERROR, "Stage error! next_stage={}", lNextStage);
    }

    assert(lNextStage >= eStfFileSinkIn && lNextStage <= eStfNullIn);

    return lNextStage;
  }

  void StfOutputThread();
  void InfoThread();

  struct StfBuilderInstance {
    /// config
    std::string mInputChannelName;
    std::string mOutputChannelName;
    std::string mDplChannelName;
    bool mStandalone;
    bool mDplEnabled;
    std::int64_t mMaxStfsInPipeline;
    std::uint64_t mMaxBuiltStfs;
    bool mPipelineLimit;

    /// Input Interface handler
    std::unique_ptr<StfInputInterface> mReadoutInterface;
    std::atomic_int64_t mNumStfs{ 0 };

    /// Internal threads
    std::thread mOutputThread;
    std::atomic_bool mRunning = true;
    std::atomic_bool mPaused = false;

    /// File sink
    std::unique_ptr<SubTimeFrameFileSink> mFileSink;

    /// File source
    std::unique_ptr<SubTimeFrameFileSource> mFileSource;

    /// Info thread
    std::thread mInfoThread;
    RunningSamples<uint64_t> mStfSizeSamples;
    RunningSamples<float> mStfDataTimeSamples;
    std::uint64_t mSentOutStfsTotal = 0;
    std::uint64_t mSentOutStfs = 0; // used to calculate the rate (pause/resume)
    double mSentOutRate = 0.;
    bool mRestartRateCounter = true;
  };

  std::unique_ptr<StfBuilderInstance> mI;
  std::unique_ptr<MemoryResources> mMemI;
  const StfBuilderInstance& I() const { return *mI; }
public:
  StfBuilderInstance& I() { return *mI; }
  MemoryResources& MemI() { return *mMemI; }
};

}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STFBUILDER_DEVICE_H_ */
