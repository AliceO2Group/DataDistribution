// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#ifndef ALICEO2_STFBUILDER_DEVICE_H_
#define ALICEO2_STFBUILDER_DEVICE_H_

#include "SubTimeFrameBuilderInput.h"

#include <ReadoutDataModel.h>
#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameFileSink.h>
#include <ConcurrentQueue.h>
#include <Utilities.h>
#include <RootGui.h>

#include <TApplication.h>
#include <TCanvas.h>
#include <TH1.h>

#include <deque>
#include <memory>
#include <mutex>
#include <condition_variable>

#include <stdexcept>

namespace o2
{
namespace DataDistribution
{

enum StfBuilderPipeline {
  eStfBuilderOut = 0,

  eStfFileSinkIn = 0,
  eStfFileSinkOut = 1,

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
  static constexpr const char* OptionKeyGui = "gui";

  /// Default constructor
  StfBuilderDevice();

  /// Default destructor
  ~StfBuilderDevice() override;

  void InitTask() final;
  void ResetTask() final;

  const std::string& getInputChannelName() const { return mInputChannelName; }
  const std::string& getOutputChannelName() const { return mOutputChannelName; }
  const std::string& getDplChannelName() const { return mDplChannelName; }

  bool guiEnabled() const noexcept { return mBuildHistograms; }
  bool dplEnabled() const noexcept { return mDplEnabled; }

 protected:
  void PreRun() final;
  void PostRun() final { };
  bool ConditionalRun() final;

  unsigned getNextPipelineStage(unsigned pStage) final
  {
    StfBuilderPipeline lNextStage = eStfInvalidStage;

    switch (pStage) {
      case eStfBuilderOut: {
        auto lNumStfs = mNumStfs.fetch_add(1) + 1; // fetch old val + 1

        if (mPipelineLimit && (lNumStfs > mMaxStfsInPipeline)) {
          mNumStfs--;
          lNextStage = eStfNullIn;
          LOG(WARNING) << "Dropping an STF due to reaching the maximum number of buffered "
                          "STFs in the process ("
                       << mMaxStfsInPipeline
                       << "). Consider increasing the limit, or reduce the data pressure.";
        } else {
          lNextStage = mFileSink.enabled() ? eStfFileSinkIn : eStfSendIn;
        }
        break;
      }
      case eStfFileSinkOut:

        lNextStage = eStfSendIn;
        break;

      default:
        throw std::runtime_error("pipeline error");
    }

    assert(lNextStage >= eStfFileSinkIn && lNextStage <= eStfSendIn);

    return lNextStage;
  }

  void StfOutputThread();

  /// config
  std::string mInputChannelName;
  std::string mOutputChannelName;
  std::string mDplChannelName;
  bool mStandalone;
  bool mDplEnabled;
  std::int64_t mMaxStfsInPipeline;
  bool mPipelineLimit;

  /// Input Interface handler
  StfInputInterface mReadoutInterface;
  std::atomic_int64_t mNumStfs{ 0 };

  /// Internal threads
  std::thread mOutputThread;

  /// File sink
  SubTimeFrameFileSink mFileSink;

  /// Root GUI stuff
  void GuiThread();
  bool mBuildHistograms = true;
  std::unique_ptr<RootGui> mGui;
  std::thread mGuiThread;

  RunningSamples<uint64_t> mStfSizeSamples;
  RunningSamples<float> mStfDataTimeSamples;
};
}
} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STFBUILDER_DEVICE_H_ */
