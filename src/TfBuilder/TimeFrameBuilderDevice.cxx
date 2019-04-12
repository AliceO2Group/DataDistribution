// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

#include "TimeFrameBuilderDevice.h"
#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameVisitors.h>

#include <options/FairMQProgOptions.h>
#include <FairMQLogger.h>

#include <TH1.h>

#include <chrono>
#include <thread>

namespace o2
{
namespace DataDistribution
{

using namespace std::chrono_literals;

TfBuilderDevice::TfBuilderDevice()
  : DataDistDevice(),
    IFifoPipeline(eTfPipelineSize),
    mFlpInputHandler(*this, eTfBuilderOut),
    mFileSink(*this, *this, eTfFileSinkIn, eTfFileSinkOut),
    mTfSizeSamples(),
    mTfFreqSamples()
{
}

TfBuilderDevice::~TfBuilderDevice()
{
}

void TfBuilderDevice::InitTask()
{
  mInputChannelName = GetConfig()->GetValue<std::string>(OptionKeyInputChannelName);
  mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);
  mFlpNodeCount = GetConfig()->GetValue<std::uint32_t>(OptionKeyFlpNodeCount);
  mBuildHistograms = GetConfig()->GetValue<bool>(OptionKeyGui);

  // File sink
  if (!mFileSink.loadVerifyConfig(*(this->GetConfig())))
    exit(-1);
}

void TfBuilderDevice::PreRun()
{
  // start TF forwarding thread
  mTfFwdThread = std::thread(&TfBuilderDevice::TfForwardThread, this);
  // start file sink
  mFileSink.start();
  // Start output handlers
  mFlpInputHandler.Start(mFlpNodeCount);

  // start the gui thread
  if (mBuildHistograms) {
    mGui = std::make_unique<RootGui>("TFBuilder", "TF Builder", 1200, 400);
    mGui->Canvas().Divide(3, 1);
    mGuiThread = std::thread(&TfBuilderDevice::GuiThread, this);
  }
}

void TfBuilderDevice::PostRun()
{
  LOG(INFO) << "PostRun() start... ";
  // Stop the pipeline
  stopPipeline();
  // stop output handlers
  mFlpInputHandler.Stop();
  // signal and wait for the output thread
  mFileSink.stop();
  // join on fwd thread
  if (mTfFwdThread.joinable()) {
    mTfFwdThread.join();
  }

  //wait for the gui thread
  if (mBuildHistograms && mGuiThread.joinable()) {
    mGuiThread.join();
  }

  LOG(INFO) << "PostRun() done... ";
}

bool TfBuilderDevice::ConditionalRun()
{
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);

  // NOTE: Not using Run or ConditionalRun lets us put teardown in PostRun()
  return true;
}

void TfBuilderDevice::TfForwardThread()
{
  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {
    auto lFreqStartTime = std::chrono::high_resolution_clock::now();

    std::unique_ptr<SubTimeFrame> lTf = dequeue(eTfFwdIn);
    if (!lTf) {
      LOG(WARNING) << "ConditionalRun(): Exiting... ";
      break;
    }

    // record frequency and size of TFs
    if (mBuildHistograms) {
      mTfFreqSamples.Fill(
        1.0 / std::chrono::duration<double>(
                std::chrono::high_resolution_clock::now() - lFreqStartTime)
                .count());

      lFreqStartTime = std::chrono::high_resolution_clock::now();

      // size histogram
      mTfSizeSamples.Fill(lTf->getDataSize());
    }

    // TODO: Do something with the TF
    {
      // is there a ratelimited LOG?
      static unsigned long floodgate = 0;
      if (++floodgate % 44 == 1)
        LOG(DEBUG) << "TF[" << lTf->header().mId << "] size: " << lTf->getDataSize();
    }
  }

  LOG(INFO) << "Exiting TF forwarding thread... ";
}

void TfBuilderDevice::GuiThread()
{
  std::unique_ptr<TH1F> lTfSizeHist = std::make_unique<TH1F>("TfSizeH", "Size of TF", 100, 0.0, float(1UL << 30));
  lTfSizeHist->GetXaxis()->SetTitle("Size [B]");

  std::unique_ptr<TH1F> lTfFreqHist = std::make_unique<TH1F>("TfFreq", "TimeFrame frequency", 200, 0.0, 100.0);
  lTfFreqHist->GetXaxis()->SetTitle("Frequency [Hz]");

  std::unique_ptr<TH1S> lStfPipelinedCntHist = std::make_unique<TH1S>("StfQueuedH", "Queued STFs", 150, -0.5, 150.0 - 0.5);
  lStfPipelinedCntHist->GetXaxis()->SetTitle("Number of queued Stf");

  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {
    LOG(INFO) << "Updating histograms...";

    mGui->Canvas().cd(1);
    mGui->DrawHist(lTfSizeHist.get(), mTfSizeSamples);

    mGui->Canvas().cd(2);
    mGui->DrawHist(lTfFreqHist.get(), mTfFreqSamples);

    mGui->Canvas().cd(3);
    mGui->DrawHist(lStfPipelinedCntHist.get(), this->getPipelinedSizeSamples());

    mGui->Canvas().Modified();
    mGui->Canvas().Update();

    std::this_thread::sleep_for(5s);
  }

  LOG(INFO) << "Exiting GUI thread...";
}
}
} /* namespace o2::DataDistribution */
