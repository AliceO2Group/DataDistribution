// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "SubTimeFrameBuilderDevice.h"

#include <SubTimeFrameUtils.h>
#include <SubTimeFrameVisitors.h>
#include <ReadoutDataModel.h>
#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameDPL.h>
#include <Utilities.h>

#include <TH1.h>

#include <options/FairMQProgOptions.h>
#include <FairMQLogger.h>

#include <chrono>
#include <thread>
#include <queue>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

constexpr int StfBuilderDevice::gStfOutputChanId;

StfBuilderDevice::StfBuilderDevice()
  : DataDistDevice(),
    IFifoPipeline(eStfPipelineSize),
    mReadoutInterface(*this),
    mFileSink(*this, *this, eStfFileSinkIn, eStfFileSinkOut),
    mGui(nullptr),
    mStfSizeSamples(),
    mStfDataTimeSamples()
{
}

StfBuilderDevice::~StfBuilderDevice()
{
}

void StfBuilderDevice::InitTask()
{
  mInputChannelName = GetConfig()->GetValue<std::string>(OptionKeyInputChannelName);
  mOutputChannelName = GetConfig()->GetValue<std::string>(OptionKeyOutputChannelName);
  mDplChannelName = GetConfig()->GetValue<std::string>(OptionKeyDplChannelName);
  mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);
  mMaxStfsInPipeline = GetConfig()->GetValue<std::int64_t>(OptionKeyMaxBufferedStfs);
  mBuildHistograms = GetConfig()->GetValue<bool>(OptionKeyGui);
  mDplEnabled = GetConfig()->GetValue<bool>(OptionKeyDpl);

  // Buffering limitation
  if (mMaxStfsInPipeline > 0) {
    if (mMaxStfsInPipeline < 4) {
      mMaxStfsInPipeline = 4;
      LOG(WARN) << "Configuration: max buffered SubTimeFrames limit increased to: " << mMaxStfsInPipeline;
    }
    mPipelineLimit = true;
    LOG(WARN) << "Configuration: Max buffered SubTimeFrames limit is set to " << mMaxStfsInPipeline
              << ". Consider increasing it if data loss occurs.";
  } else {
    mPipelineLimit = false;
    LOG(INFO) << "Not imposing limits on number of buffered SubTimeFrames. "
                 "Possibility of creating back-pressure.";
  }

  // File sink
  if (!mFileSink.loadVerifyConfig(*(this->GetConfig())))
    exit(-1);

  // ChannelAllocator::get().addChannel(gStfOutputChanId, GetChannel(mOutputChannelName, 0));

  // check if output enabled
  if (mStandalone && !mFileSink.enabled()) {
    LOG(WARNING) << "Running in standalone mode and with STF file sink disabled. "
                    "Data will be lost.";
  }
}

void StfBuilderDevice::PreRun()
{
  // start output thread
  mOutputThread = std::thread(&StfBuilderDevice::StfOutputThread, this);
  // start file sink
  mFileSink.start();
  // start a thread for readout process
  mReadoutInterface.Start();

  // gui thread
  if (mBuildHistograms) {
    mGui = std::make_unique<RootGui>("STFBuilder", "STF Builder", 1600, 400);
    mGui->Canvas().Divide(4, 1);
    mGuiThread = std::thread(&StfBuilderDevice::GuiThread, this);
  }

  LOG(INFO) << "PreRun() done... ";
}

void StfBuilderDevice::ResetTask()
{
  // Stop the pipeline
  stopPipeline();
  // wait for readout interface threads
  mReadoutInterface.Stop();
  // signal and wait for the output thread
  mFileSink.stop();
  // stop the output
  if (mOutputThread.joinable()) {
    mOutputThread.join();
  }

  // wait for the gui thread
  if (mBuildHistograms && mGuiThread.joinable()) {
    mGuiThread.join();
  }

  LOG(INFO) << "ResetTask() done... ";
}

void StfBuilderDevice::StfOutputThread()
{
  // wait for the device to go into RUNNING state
  WaitForRunningState();

  auto& lOutputChan = GetChannel(getOutputChannelName(), 0);
  InterleavedHdrDataSerializer lStfSerializer(lOutputChan);

  auto& lDplChan = GetChannel(getDplChannelName(), 0);
  StfDplAdapter lStfDplAdapter(lDplChan);

  using hres_clock = std::chrono::high_resolution_clock;

  while (IsRunningState()) {

    // Get a STF readu for sending
    std::unique_ptr<SubTimeFrame> lStf = dequeue(eStfSendIn);
    if (!lStf)
      break;

    // decrement the stf counter
    mNumStfs--;

    // Send the STF
    const auto lSendStartTime = hres_clock::now();

#ifdef STF_FILTER_EXAMPLE
    // EXAMPLE:
    // split one SubTimeFrame into per detector SubTimeFrames (this is unlikely situation for a STF
    // but still... The TimeFrame structure should be similar)
    DataIdentifier cTPCDataIdentifier;
    cTPCDataIdentifier.dataDescription = gDataDescriptionAny;
    cTPCDataIdentifier.dataOrigin = gDataOriginTPC;
    DataIdentifier cITSDataIdentifier;
    cITSDataIdentifier.dataDescription = gDataDescriptionAny;
    cITSDataIdentifier.dataOrigin = gDataOriginITS;

    DataIdentifierSplitter lStfDetectorSplitter;
    auto lStfTPC = lStfDetectorSplitter.split(*lStf, cTPCDataIdentifier);
    auto lStfITS = lStfDetectorSplitter.split(*lStf, cITSDataIdentifier);

    if (mBuildHistograms) {
      mStfSizeSamples.Fill(lStfTPC->getDataSize());
      mStfSizeSamples.Fill(lStfITS->getDataSize());
      mStfSizeSamples.Fill(lStf->getDataSize());
    }

    if (!mStandalone) {
      // Send filtered data as two objects
      try {
        if (!mDplEnabled) {
          lStfSerializer.serialize(std::move(lStfTPC)); // TPC data
          lStfSerializer.serialize(std::move(lStfITS)); // ITS data
          lStfSerializer.serialize(std::move(lStf));    // whatever is left
        } else {
          lStfDplAdapter.sendToDpl(std::move(lStfTPC));
          lStfDplAdapter.sendToDpl(std::move(lStfITS));
          lStfDplAdapter.sendToDpl(std::move(lStf));
        }
      } catch (std::exception& e) {
        if (IsRunningState())
          LOG(ERROR) << "StfOutputThread: exception on send: " << e.what();
        else
          LOG(INFO) << "StfOutputThread(NOT_RUNNING): exception on send: " << e.what();
        break;
      }
    }
#else
    if (mBuildHistograms) {
      mStfSizeSamples.Fill(lStf->getDataSize());
    }

    if (!mStandalone) {
      try {
        if (!mDplEnabled) {
          lStfSerializer.serialize(std::move(lStf));
        } else {

          // DPL Channel
          static thread_local unsigned long lThrottle = 0;
          if (++lThrottle % 88 == 0) {
            LOG(DEBUG) << "Sending STF to DPL: id:" << lStf->header().mId
                       << " data size: " << lStf->getDataSize()
                       << " unique equipments: " << lStf->getEquipmentIdentifiers().size();
          }

          // Send to DPL bridge
          lStfDplAdapter.sendToDpl(std::move(lStf));
        }
      } catch (std::exception& e) {
        if (IsRunningState()) {
          LOG(ERROR) << "StfOutputThread: exception on send: " << e.what();
        } else {
          LOG(INFO) << "StfOutputThread(NOT_RUNNING): shutting down: " << e.what();
        }
        break;
      }
    }
#endif

    if (!mStandalone && mBuildHistograms) {
      double lTimeMs = std::chrono::duration<double, std::milli>(hres_clock::now() - lSendStartTime).count();
      mStfDataTimeSamples.Fill(lTimeMs);
    }
  }

  LOG(INFO) << "Exiting StfOutputThread...";
}

void StfBuilderDevice::GuiThread()
{
  std::unique_ptr<TH1F> lStfSizeHist = std::make_unique<TH1F>("StfSizeH", "Readout data size per STF", 100, 0.0, 400e+6);
  lStfSizeHist->GetXaxis()->SetTitle("Size [B]");

  std::unique_ptr<TH1F> lStfFreqHist = std::make_unique<TH1F>("STFFreq", "SubTimeFrame frequency", 1000, 0.0, 200.0);
  lStfFreqHist->GetXaxis()->SetTitle("Frequency [Hz]");

  std::unique_ptr<TH1F> lStfDataTimeHist = std::make_unique<TH1F>("StfChanTimeH", "STF on-channel time", 100, 0.0, 20.0);
  lStfDataTimeHist->GetXaxis()->SetTitle("Time [ms]");

  std::unique_ptr<TH1S> lStfPipelinedCntHist = std::make_unique<TH1S>("StfQueuedH", "Queued STFs", 150, -0.5, 150.0 - 0.5);
  lStfPipelinedCntHist->GetXaxis()->SetTitle("Number of queued Stf");

  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {
    LOG(INFO) << "Updating histograms...";

    mGui->Canvas().cd(1);
    mGui->DrawHist(lStfSizeHist.get(), mStfSizeSamples);

    mGui->Canvas().cd(2);
    mGui->DrawHist(lStfFreqHist.get(), mReadoutInterface.StfFreqSamples());

    mGui->Canvas().cd(3);
    mGui->DrawHist(lStfDataTimeHist.get(), mStfDataTimeSamples);

    mGui->Canvas().cd(4);
    mGui->DrawHist(lStfPipelinedCntHist.get(), this->getPipelinedSizeSamples());

    mGui->Canvas().Modified();
    mGui->Canvas().Update();

    std::this_thread::sleep_for(5s);
  }
  LOG(INFO) << "Exiting GUI thread...";
}

bool StfBuilderDevice::ConditionalRun()
{
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(1s);
  return true;
}
}
} /* namespace o2::DataDistribution */
