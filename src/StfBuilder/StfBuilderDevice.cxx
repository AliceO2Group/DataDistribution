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

#include "StfBuilderDevice.h"

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
#include <exception>
#include <boost/algorithm/string.hpp>

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
    mFileSource(*this, eStfFileSourceOut),
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
  mDataOrigin = getDataOriginFromOption(GetConfig()->GetValue<std::string>(OptionKeyStfDetector));

  // input data handling
  ReadoutDataUtils::setRdhSanityCheckMode(
    GetConfig()->GetValue<ReadoutDataUtils::SanityCheckMode>(OptionKeyRdhSanityCheck)
  );
  mRdh4FilterTrigger = GetConfig()->GetValue<bool>(OptionKeyFilterTriggerRdh4);

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
  if (!mFileSink.loadVerifyConfig(*(this->GetConfig()))) {
    exit(-1);
  }

  // File source
  if (!mFileSource.loadVerifyConfig(*(this->GetConfig()))) {
    exit(-1);
  }

  // make sure we have detector if not using files
  if (!mFileSource.enabled()) {
    if (mDataOrigin == gDataOriginInvalid) {
      LOG(ERROR) << "Detector string parameter not specified (required when not using file source).";
      exit(-1);
    }

    if (ReadoutDataUtils::getRdhSanityCheckMode()) {
      LOG(INFO) << "Extensive RDH checks enabled. Data that does not meet the criteria will be dropped.";
    }

    if (mRdh4FilterTrigger) {
      LOG(INFO) << "Filtering of empty HBFrames in triggered mode enabled for RDHv4.";
    }
  }

  // check if output enabled
  if (mStandalone && !mFileSink.enabled()) {
    LOG(WARNING) << "Running in standalone mode and with STF file sink disabled. "
                    "Data will be lost.";
  }

  // Using DPL?
  if (mDplChannelName != "" && !mStandalone) {
    mDplEnabled = true;
    LOG(INFO) << "DPL Channel name: " << mDplChannelName;
  } else {
    mDplEnabled = false;
    LOG(INFO) << "Not sending to DPL.";
  }
}

void StfBuilderDevice::PreRun()
{
  // try to see if channels have been configured
  {
    if (!mFileSource.enabled()) {
      try {
        GetChannel(mInputChannelName);
      } catch(std::exception &) {
        LOG(ERROR) << "Input channel not configured (from readout.exe) and not running with file source enabled.";
        exit(-1);
      }
    }

    try {
      if (!mStandalone) {
        GetChannel(mDplEnabled ? mDplChannelName : mOutputChannelName);
      }
    } catch(std::exception &) {
      LOG(ERROR) << "Output channel not configured (to DPL or StfSender) and not running in standalone mode.";
      exit(-1);
    }
  }

  // start output thread
  mOutputThread = std::thread(&StfBuilderDevice::StfOutputThread, this);
  // start file sink
  mFileSink.start();

  // start file source
  // channel for FileSource: stf or dpl, or generic one in case of standalone
  if (mStandalone) {
    // create default FMQ shm channel
    auto lTransportFactory = FairMQTransportFactory::CreateTransportFactory("shmem", "", GetConfig());
    if (!lTransportFactory) {
      LOG(ERROR) << "Creating transport factory failed!";
      exit(-1);
    }
    mStandaloneChannel = std::make_unique<FairMQChannel>(
      "standalone-chan[0]" ,  // name
      "pair",              // type
      "bind",              // method
      "ipc:///tmp/standalone-chan-stfb", // address
      lTransportFactory
    );

    // mStandaloneChannel.Init();
    mStandaloneChannel->Init();
    // mStandaloneChannel->BindEndpoint("ipc:///tmp/standalone-chan");
    mStandaloneChannel->Validate();
    mFileSource.start(*mStandaloneChannel, mDplEnabled);
  } else {
    mFileSource.start(GetChannel(mDplEnabled ? mDplChannelName : mOutputChannelName), mDplEnabled);
  }


  // start a thread for readout process
  if (!mFileSource.enabled()) {
    mReadoutInterface.setRdh4FilterTrigger(mRdh4FilterTrigger);
    mReadoutInterface.start(mDataOrigin);
  }

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
  // signal and wait for the output thread
  mFileSource.stop();

  // Stop the pipeline
  stopPipeline();

  // wait for readout interface threads
  if (!mFileSource.enabled()) {
    mReadoutInterface.stop();
  }

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

  std::unique_ptr<InterleavedHdrDataSerializer> lStfSerializer;
  std::unique_ptr<StfDplAdapter> lStfDplAdapter;

  // cannot get the channels in standalone mode
  if (!mStandalone) {
    if (!dplEnabled()) {
      auto& lOutputChan = GetChannel(getOutputChannelName(), 0);
      lStfSerializer = std::make_unique<InterleavedHdrDataSerializer>(lOutputChan);
    } else {
      auto& lOutputChan = GetChannel(getDplChannelName(), 0);
      lStfDplAdapter = std::make_unique<StfDplAdapter>(lOutputChan);
    }
  }

  while (IsRunningState()) {
  using hres_clock = std::chrono::high_resolution_clock;

    // Get a STF ready for sending
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
        if (!dplEnabled()) {
          assert (lStfSerializer);
          lStfSerializer->serialize(std::move(lStfTPC)); // TPC data
          lStfSerializer->serialize(std::move(lStfITS)); // ITS data
          lStfSerializer->serialize(std::move(lStf));    // whatever is left
        } else {
          assert (lStfDplAdapter);
          lStfDplAdapter->sendToDpl(std::move(lStfTPC));
          lStfDplAdapter->sendToDpl(std::move(lStfITS));
          lStfDplAdapter->sendToDpl(std::move(lStf));
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

        if (!dplEnabled()) {
          assert (lStfSerializer);
          lStfSerializer->serialize(std::move(lStf));
        } else {

          // DPL Channel
          static thread_local unsigned long lThrottle = 0;
          if (++lThrottle % 88 == 0) {
            LOG(DEBUG) << "Sending STF to DPL: id:" << lStf->header().mId
                       << " data size: " << lStf->getDataSize()
                       << " unique equipments: " << lStf->getEquipmentIdentifiers().size();
          }

          // Send to DPL bridge
          assert (lStfDplAdapter);
          lStfDplAdapter->sendToDpl(std::move(lStf));
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
  lStfPipelinedCntHist->GetXaxis()->SetTitle("Number of queued STFs");

  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {

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

    LOG(INFO) << "Readout data size per STF: " << mStfSizeSamples.Mean();
    LOG(INFO) << "SubTimeFrame frequency   : " << mReadoutInterface.StfFreqSamples().Mean();
    LOG(INFO) << "Queued STFs in StfBuilder: " << mNumStfs;

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


bpo::options_description StfBuilderDevice::getDetectorProgramOptions() {
  bpo::options_description lDetectorOptions("SubTimeFrameBuilder data source", 120);

  lDetectorOptions.add_options() (
    OptionKeyStfDetector,
    bpo::value<std::string>()->default_value(""),
    "Specifies the detector string for SubTimeFrame building. Allowed are: "
    "ACO, CPV, CTP, EMC, FT0, FV0, FDD, HMP, ITS, MCH, MFT, MID, PHS, TOF, TPC, TRD, ZDC."
  );

  return lDetectorOptions;
}

bpo::options_description StfBuilderDevice::getStfBuildingProgramOptions() {
  bpo::options_description lStfBuildingOptions("Options controlling SubTimeFrame building", 120);

  lStfBuildingOptions.add_options() (
    OptionKeyRdhSanityCheck,
    bpo::value<ReadoutDataUtils::SanityCheckMode>()->default_value(ReadoutDataUtils::SanityCheckMode::eNoSanityCheck, "off"),
    "Enable extensive RDH verification. Permitted values: off, print, drop (caution, any data not meeting criteria will be dropped)")(
    OptionKeyFilterTriggerRdh4,
    bpo::bool_switch()->default_value(false),
    "Filter out empty HBFrames with RDHv4 sent in triggered mode.");

  return lStfBuildingOptions;
}



o2::header::DataOrigin StfBuilderDevice::getDataOriginFromOption(const std::string pArg)
{
  const auto lDetStr = boost::to_upper_copy<std::string>(pArg);

  if (lDetStr == "ACO") {
    return o2::header::gDataOriginACO;
  }
  else if (lDetStr == "CPV") {
    return o2::header::gDataOriginCPV;
  }
  else if (lDetStr == "CTP") {
    return o2::header::gDataOriginCTP;
  }
  else if (lDetStr == "EMC") {
    return o2::header::gDataOriginEMC;
  }
  else if (lDetStr == "FT0") {
    return o2::header::gDataOriginFT0;
  }
  else if (lDetStr == "FV0") {
    return o2::header::gDataOriginFV0;
  }
  else if (lDetStr == "FDD") {
    return o2::header::gDataOriginFDD;
  }
  else if (lDetStr == "HMP") {
    return o2::header::gDataOriginHMP;
  }
  else if (lDetStr == "ITS") {
    return o2::header::gDataOriginITS;
  }
  else if (lDetStr == "MCH") {
    return o2::header::gDataOriginMCH;
  }
  else if (lDetStr == "MFT") {
    return o2::header::gDataOriginMFT;
  }
  else if (lDetStr == "MID") {
    return o2::header::gDataOriginMID;
  }
  else if (lDetStr == "PHS") {
    return o2::header::gDataOriginPHS;
  }
  else if (lDetStr == "TOF") {
    return o2::header::gDataOriginTOF;
  }
  else if (lDetStr == "TPC") {
    return o2::header::gDataOriginTPC;
  }
  else if (lDetStr == "TRD") {
    return o2::header::gDataOriginTRD;
  }
  else if (lDetStr == "ZDC") {
    return o2::header::gDataOriginZDC;
  }

  return o2::header::gDataOriginInvalid;
}


}
} /* namespace o2::DataDistribution */
