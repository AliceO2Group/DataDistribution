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

#ifndef ALICEO2_STF_SENDER_DEVICE_H_
#define ALICEO2_STF_SENDER_DEVICE_H_

#include "StfSenderRpc.h"
#include "StfSenderOutput.h"

#include <ConfigConsul.h>
#include <TfSchedulerRpcClient.h>

#include <SubTimeFrameFileSink.h>
#include <Utilities.h>
#include <FmqUtilities.h>
#include <DataDistMonitoring.h>

#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>

namespace o2::DataDistribution
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

class StfSenderDevice : public DataDistDevice
{
 public:
  static constexpr const char* OptionKeyInputChannelName = "input-channel-name";
  static constexpr const char* OptionKeyStandalone = "stand-alone";
  static constexpr const char* OptionKeyDataRegionSize = "dd-region-size";
  static constexpr const char* OptionKeyDataRegionId = "dd-region-id";

  /// Default constructor
  StfSenderDevice();

  /// Default destructor
  ~StfSenderDevice() override;

  bool standalone() const { return I().mStandalone; }
  bool running() const  { return I().mRunning; }
  bool deviceRunning() const { return I().mDeviceRunning; }
  bool acceptingData() const { return I().mAcceptingData; }

  TfSchedulerRpcClient& TfSchedRpcCli() { return I().mTfSchedulerRpcClient; }

 protected:
  virtual void Init() override final;
  virtual void Reset() override final;

  virtual void InitTask() override final;
  virtual void ResetTask() override final;

  void AbortInitTask() {
    DDDLOG("Aborting InitTask...");
    if (mI && I().mDiscoveryConfig) {
      auto& lStatus = I().mDiscoveryConfig->status();
      lStatus.mutable_info()->set_process_state(BasicInfo::ABORTED);
      I().mDiscoveryConfig->write();
    }
    ResetTask();
  }

  virtual void PreRun() final;
  virtual void PostRun() final;
  virtual bool ConditionalRun() final;

  void StfReceiverThread();
  void InfoThread();

  struct StfSenderInstance : public IFifoPipeline<std::unique_ptr<SubTimeFrame>> {

    StfSenderInstance()
    : IFifoPipeline(ePipelineSize) {}

    ~StfSenderInstance() {
      mRunning = false;
      mDeviceRunning = false;

      // stop the info thread
      if (mInfoThread.joinable()) {
        mInfoThread.join();
      }

      // stop the receiver thread
      if (mReceiverThread.joinable()) {
        mReceiverThread.join();
      }
    }

    /// Configuration
    std::string mInputChannelName;
    bool mStandalone = false;
    std::string mPartitionId;

    /// Discovery configuration
    std::shared_ptr<ConsulStfSender> mDiscoveryConfig;

    /// Scheduler RPC client
    TfSchedulerRpcClient mTfSchedulerRpcClient;

    /// StfCopy region
    std::uint64_t mDataRegionSize = std::uint64_t(32) << 30;
    std::optional<std::uint16_t> mDataRegionId = std::nullopt;

    /// Receiver threads
    bool mRunning = false;
    bool mDeviceRunning = true;
    bool mAcceptingData = false;
    std::uint64_t mLastStfId = 0;
    std::thread mReceiverThread;

    /// File sink
    std::unique_ptr<SubTimeFrameFileSink> mFileSink;

    /// StfCopy builder
    std::shared_ptr<SubTimeFrameCopyBuilder> mStfCopyBuilder;

    /// Output stage handler
    std::unique_ptr<StfSenderOutput> mOutputHandler;

    /// RPC service
    std::unique_ptr<StfSenderRpcImpl> mRpcServer;

    /// Info thread
    std::thread mInfoThread;

    unsigned getNextPipelineStage(unsigned pStage) final
    {
      StfSenderPipeline lNextStage = eInvalidStage;
      switch (pStage) {
        case eReceiverOut:
        {
          if (mFileSink->enabled()) {
            lNextStage = eFileSinkIn;
            break;
          }

          lNextStage = eSenderIn;
          break;
        }
        case eFileSinkOut:
        {
          lNextStage = eSenderIn;
          break;
        }

        default:
          throw std::runtime_error("pipeline error");
      }

      assert(lNextStage >= eFileSinkIn && lNextStage <= eNullIn);
      return lNextStage;
    }
  };

  std::unique_ptr<StfSenderInstance> mI;
  std::unique_ptr<SyncMemoryResources> mMemI;
  const StfSenderInstance& I() const { return *mI; }

public:
  StfSenderInstance& I() { return *mI; }
  SyncMemoryResources& MemI() { return *mMemI; }
};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STF_SENDER_DEVICE_H_ */
