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

#ifndef ALICEO2_STFBUILDER_INPUT_H_
#define ALICEO2_STFBUILDER_INPUT_H_

#include <DataDistMonitoring.h>
#include <SubTimeFrameBuilder.h>
#include <ConcurrentQueue.h>
#include <Utilities.h>
#include <ConfigConsul.h>

#include <Headers/DataHeader.h>

#include <thread>
#include <vector>

namespace o2::DataDistribution
{

class StfBuilderDevice;

class StfInputInterface
{

public:
  static constexpr uint8_t sReadoutInterfaceVersion = 2;

  StfInputInterface() = delete;
  StfInputInterface(StfBuilderDevice &pStfBuilderDev)
    : mDevice(pStfBuilderDev)
  { }
  void start(bool pBuildStf, std::shared_ptr<ConsulStfBuilder> pDiscoveryConfig);
  void stop();

  void setRunningState(bool pRunning) {
    // reset counters when starting the run
    if (pRunning && !mAcceptingData) {
      mStfIdReceiving = 0;
      mStfIdBuilding = 0;
      mLastSeqStfId = 0;
      mMissingStfs = 0;
    }

    mAcceptingData = pRunning;

    DDMON("stfbuilder", "stf_input.missing_stf.total", 0);
  }

  void StfReceiverThread();
  void StfBuilderThread();
  void TopologicalStfBuilderThread(); // threshold scans
  void StfSequencerThread();

 private:
  /// Main SubTimeBuilder O2 device
  StfBuilderDevice &mDevice;

  /// Discovery configuration
  std::shared_ptr<ConsulStfBuilder> mDiscoveryConfig;

  /// Thread for the input channel
  bool mRunning = false;
  bool mAcceptingData = false;
  bool mBuildStf = true; // STF or equipment (threshold scan)
  std::uint32_t mStfIdReceiving = 0;
  std::thread mInputThread;

  /// StfBuilding thread and queues
  std::unique_ptr<ConcurrentFifo<std::vector<fair::mq::MessagePtr>>> mBuilderInputQueue = nullptr;
  std::unique_ptr<SubTimeFrameReadoutBuilder> mStfBuilder = nullptr;
  std::uint32_t mStfIdBuilding = 0;
  std::thread mBuilderThread;

  /// StfSequencer thread
  std::thread mStfSeqThread;
  ConcurrentFifo<std::unique_ptr<SubTimeFrame>> mSeqStfQueue;
  std::uint64_t mLastSeqStfId = 0;
  std::uint64_t mMissingStfs = 0;

};

} /* namespace o2::DataDistribution */

#endif /* ALICEO2_STFBUILDER_INPUT_H_ */
