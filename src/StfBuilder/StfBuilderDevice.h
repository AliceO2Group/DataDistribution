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

#ifndef ALICEO2_STFBUILDER_DEVICE_H_
#define ALICEO2_STFBUILDER_DEVICE_H_

#include "StfBuilderInput.h"

#include <ReadoutDataModel.h>
#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameFileSink.h>
#include <SubTimeFrameFileSource.h>
#include <ConcurrentQueue.h>
#include <Utilities.h>
#include <FmqUtilities.h>
#include <DataDistMonitoring.h>

#include <deque>
#include <memory>
#include <mutex>
#include <condition_variable>

#include <stdexcept>

#include <DataDistLogger.h>
#include <ConfigConsul.h>

namespace o2::DataDistribution
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

class StfBuilderDevice : public DataDistDevice
{
 public:
  constexpr static int gStfOutputChanId = 0;

  static constexpr const char* OptionKeyInputChannelName = "input-channel-name";
  static constexpr const char* OptionKeyOutputChannelName = "output-channel-name";
  static constexpr const char* OptionKeyDplChannelName = "dpl-channel-name";
  static constexpr const char* OptionKeyRunType = "run-type";
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

  bool isStandalone() const noexcept { return I().mStandalone; }

  const std::string& getInputChannelName() const { return I().mInputChannelName; }
  const std::string& getDplChannelName() const { return I().mDplChannelName; }

  auto& getOutputChannel() {
    if (!I().mDplChannelName.empty()) {
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
  virtual void PreRun() override final;
  virtual void PostRun() override final;

  void StfOutputThread();
  void InfoThread();

  struct StfBuilderInstance : public IFifoPipeline<std::unique_ptr<SubTimeFrame>> {

    StfBuilderInstance()
    : IFifoPipeline(eStfPipelineSize) {}

    /// config
    std::string mPartitionId;
    std::string mInputChannelName;
    std::string mOutputChannelName;
    std::string mDplChannelName;
    bool mStandalone;
    std::int64_t mMaxStfsInPipeline;
    std::uint64_t mMaxBuiltStfs;
    bool mPipelineLimit;

    /// Discovery configuration
    std::shared_ptr<ConsulStfBuilder> mDiscoveryConfig;

    /// Input Interface handler
    std::unique_ptr<StfInputInterface> mReadoutInterface;
    struct alignas(64) {
      std::atomic_int64_t mNumStfs{ 0 };
    } mCounters;

    /// Internal threads
    std::thread mOutputThread;
    struct alignas(64) {
      std::atomic_bool mRunning = false;         // Task initialized
      std::atomic_bool mInRunningState = false;  // FMQ in running state
      std::atomic_bool mPaused = false;          // paused state for File reader
    } mState;

    /// File sink
    std::unique_ptr<SubTimeFrameFileSink> mFileSink;

    /// File source
    std::unique_ptr<SubTimeFrameFileSource> mFileSource;

    std::uint64_t mSentOutStfsTotal = 0;
    std::uint64_t mSentOutStfs = 0; // used to calculate the rate (pause/resume)
    double mSentOutRate = 0.0;
    bool mRestartRateCounter = true;


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

    unsigned getNextPipelineStage(unsigned pStage)
    {
    StfBuilderPipeline lNextStage = eStfInvalidStage;

    switch (pStage) {
      case eStfBuilderOut:
      /* case eStfFileSourceOut: */
      {
        mCounters.mNumStfs++;

        if (mPipelineLimit && (mCounters.mNumStfs >= mMaxStfsInPipeline)) {

          // DROP policy in StfBuilder is to keep most current STFs. This will ensure that all
          // StfBuilders have the same set of STFs ready for distribution

          WDDLOG_RL(500, "Dropping oldest STF due to reaching the maximum number of buffered "
            "STFs in the process ({}). Consider increasing the limit, or reducing the input data rate.",
            mMaxStfsInPipeline);

          if (tryPopOldestStfs()) {
            mCounters.mNumStfs--;
          }
        }

        if (mFileSink->enabled()) {
          mCounters.mNumStfs--;
          lNextStage = eStfFileSinkIn;
        } else {
          lNextStage = eStfSendIn;
        }
        break;
      }
      case eStfFileSinkOut:
      {
        mCounters.mNumStfs++;
        lNextStage = eStfSendIn;
        break;
      }
      default:
        throw std::runtime_error("pipeline error");
    }

    if (!(lNextStage >= eStfFileSinkIn && lNextStage <= eStfNullIn)) {
      EDDLOG("Stage error! next_stage={}", (int)lNextStage);
    }

    assert(lNextStage >= eStfFileSinkIn && lNextStage <= eStfNullIn);

    return lNextStage;
  }
  };

  std::unique_ptr<StfBuilderInstance> mI;
  std::unique_ptr<SyncMemoryResources> mMemI;
  const StfBuilderInstance& I() const { return *mI; }
public:
  StfBuilderInstance& I() { return *mI; }
  SyncMemoryResources& MemI() { return *mMemI; }
};


} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STFBUILDER_DEVICE_H_ */
