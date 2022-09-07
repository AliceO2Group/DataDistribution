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

#ifndef ALICEO2_TF_BUILDER_DEVICE_H_
#define ALICEO2_TF_BUILDER_DEVICE_H_

#include "TfBuilderInput.h"
#include "TfBuilderRpc.h"

#include <ConfigConsul.h>

#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameFileSink.h>
#include <SubTimeFrameFileSource.h>
#include <ConcurrentQueue.h>
#include <Utilities.h>
#include <FmqUtilities.h>
#include <DataDistMonitoring.h>

#include <deque>
#include <mutex>
#include <memory>
#include <condition_variable>

namespace o2::DataDistribution
{

class ConsulConfig;
class StfToDplAdapter;

enum TfBuilderPipeline {
  eTfBuilderOut = 0,

  // input/output stages
  eTfFileSinkIn = 0,
  eTfFileSinkOut = 1,

  eTfFwdIn = 1,

  eTfPipelineSize = 2,
  eTfInvalidStage = -1,
};

class TfBuilderDevice : public DataDistDevice,
                        public IFifoPipeline<std::unique_ptr<SubTimeFrame>>
{
 public:
  static constexpr const char* OptionKeyStandalone = "stand-alone";
  static constexpr const char* OptionKeyTfDataRegionSize = "tf-data-region-size";
  static constexpr const char* OptionKeyTfDataRegionId = "tf-data-region-id";
  static constexpr const char* OptionKeyTfHdrRegionSize = "tf-hdr-region-size";
  static constexpr const char* OptionKeyTfHdrRegionId = "tf-hdr-region-id";
  static constexpr const char* OptionKeyDplChannelName = "dpl-channel-name";

  /// Default constructor
  TfBuilderDevice();

  /// Default destructor
  ~TfBuilderDevice() override;

  void stop();

  void PostRun() override final;

  void Init() override final;
  void Reset() override final;

  void InitTask() final;
  bool mInitTaskFinished = false;

  void AbortInitTask() {
    DDDLOG("Aborting InitTask...");
    if (mDiscoveryConfig) {
      auto& lStatus = mDiscoveryConfig->status();
      lStatus.mutable_info()->set_process_state(BasicInfo::ABORTED);
      mDiscoveryConfig->write();
    }
    ResetTask();
  }
  void ResetTask() final;

  SyncMemoryResources& MemI() { return *mMemI; }
  TimeFrameBuilder& TfBuilderI() const { return *mTfBuilder; }

 protected:
  void PreRun() final;
  bool ConditionalRun() final;

  // Run the TFBuilder pipeline
  unsigned getNextPipelineStage(unsigned pStage) final
  {
    TfBuilderPipeline lNextStage = eTfInvalidStage;

    switch (pStage) {
      case eTfBuilderOut:
      /*case eTfFileSourceOut:*/
      {
        lNextStage = mFileSink.enabled() ? eTfFileSinkIn : eTfFwdIn;
        break;
      }
      case eTfFileSinkOut:
      {
        lNextStage = eTfFwdIn;
        break;
      }
      default:
      {
        throw std::runtime_error("pipeline error");
      }
    }

    assert(lNextStage >= eTfFileSinkIn && lNextStage <= eTfFwdIn);

    return lNextStage;
  }

  void TfForwardThread();
  void InfoThread();

  const std::string& getDplChannelName() const { return mDplChannelName; }

  /// Memory region singletons
  std::unique_ptr<SyncMemoryResources> mMemI;

  /// Configuration
  std::string mDplChannelName;
  bool mStandalone;
  std::uint64_t mTfDataRegionSize;
  std::optional<std::uint16_t> mTfDataRegionId = std::nullopt;
  std::uint64_t mTfHdrRegionSize;
  std::optional<std::uint16_t> mTfHdrRegionId = std::nullopt;
  std::string mPartitionId;

  /// Discovery configuration
  std::shared_ptr<ConsulTfBuilder> mDiscoveryConfig;

  /// RPC service
  std::shared_ptr<TfBuilderRpcImpl> mRpc;

  /// Input Interface handler
  std::unique_ptr<TfBuilderInput> mFlpInputHandler;
  /// prepare TF for output (standard or DPL)
  std::unique_ptr<TimeFrameBuilder> mTfBuilder;
  /// Serializer for DPL channel
  std::unique_ptr<StfToDplAdapter> mTfDplAdapter;

  /// File sink
  SubTimeFrameFileSink mFileSink;

  /// Info thread
  std::thread mInfoThread;

  /// TF forwarding thread
  std::thread mTfFwdThread;
  std::uint64_t mTfFwdTotalDataSize;
  std::uint64_t mTfFwdTotalTfCount;

  std::atomic_bool mRunning = false;         // Task initialized
  std::atomic_bool mDeviceRunning = true;    // Device running
  std::atomic_bool mInRunningState = false;  // FMQ in running state
  std::atomic_bool mShouldExit = false;
  std::atomic_bool mShouldSendEos = false; // toggle in post run
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_TF_BUILDER_DEVICE_H_ */
