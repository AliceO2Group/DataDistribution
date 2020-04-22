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

#include <options/FairMQProgOptions.h>

#include <chrono>
#include <thread>
#include <exception>
#include <boost/algorithm/string.hpp>

#include <DataDistLogger.h>

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
    mStfSizeSamples(),
    mStfDataTimeSamples()
{
}

StfBuilderDevice::~StfBuilderDevice()
{
}

void StfBuilderDevice::InitTask()
{
  DataDistLogger::SetThreadName("stfb-main");

  mInputChannelName = GetConfig()->GetValue<std::string>(OptionKeyInputChannelName);
  mOutputChannelName = GetConfig()->GetValue<std::string>(OptionKeyOutputChannelName);
  mDplChannelName = GetConfig()->GetValue<std::string>(OptionKeyDplChannelName);
  mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);
  mMaxStfsInPipeline = GetConfig()->GetValue<std::int64_t>(OptionKeyMaxBufferedStfs);

  // input data handling
  mDataOrigin = getDataOriginFromOption(
    GetConfig()->GetValue<std::string>(OptionKeyStfDetector));

  ReadoutDataUtils::sRdhVersion =
    GetConfig()->GetValue<ReadoutDataUtils::RdhVersion>(OptionKeyRhdVer);

  ReadoutDataUtils::sRawDataSubspectype =
    GetConfig()->GetValue<ReadoutDataUtils::SubSpecMode>(OptionKeySubSpec);

  ReadoutDataUtils::sRdhSanityCheckMode =
    GetConfig()->GetValue<ReadoutDataUtils::SanityCheckMode>(OptionKeyRdhSanityCheck);

  ReadoutDataUtils::sEmptyTriggerHBFrameFilterring =
    GetConfig()->GetValue<bool>(OptionKeyFilterEmptyTriggerData);

  // Buffering limitation
  if (mMaxStfsInPipeline > 0) {
    if (mMaxStfsInPipeline < 4) {
      mMaxStfsInPipeline = 4;
      DDLOG(fair::Severity::WARNING) << "Configuration: max buffered SubTimeFrames limit increased to: " << mMaxStfsInPipeline;
    }
    mPipelineLimit = true;
    DDLOG(fair::Severity::WARNING) << "Configuration: Max buffered SubTimeFrames limit is set to " << mMaxStfsInPipeline
              << ". Consider increasing it if data loss occurs.";
  } else {
    mPipelineLimit = false;
    DDLOG(fair::Severity::info) << "Not imposing limits on number of buffered SubTimeFrames. "
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
      DDLOG(fair::Severity::ERROR) << "Detector string parameter must be specified when receiving the data from the readout.";
      exit(-1);
    } else {
      DDLOGF(fair::Severity::info, "READOUT INTERFACE: Configured detector: {}", mDataOrigin.str);
    }

    if (ReadoutDataUtils::sRdhVersion == ReadoutDataUtils::RdhVersion::eRdhInvalid) {
      DDLOGF(fair::Severity::FATAL, "RDH version must be specified when receiving the data from the readout.");
      exit(-1);
    } else {
      DDLOGF(fair::Severity::info, "READOUT INTERFACE: Configured RDHv{}", ReadoutDataUtils::sRdhVersion);
      RDHReader::Initialize(unsigned(ReadoutDataUtils::sRdhVersion));
    }

    DDLOGF(fair::Severity::info, "READOUT INTERFACE: Configured O2 SubSpec mode: {}", to_string(ReadoutDataUtils::sRawDataSubspectype));

    if (ReadoutDataUtils::sRdhSanityCheckMode) {
      DDLOGF(fair::Severity::info, "Extensive RDH checks enabled. Data that does not meet the criteria will be {}.",
        (ReadoutDataUtils::sRdhSanityCheckMode == ReadoutDataUtils::eSanityCheckDrop ? "dropped" : "kept"));
    }

    if (ReadoutDataUtils::sEmptyTriggerHBFrameFilterring) {
      DDLOG(fair::Severity::info) << "Filtering of empty HBFrames in triggered mode enabled.";
    }
  }

  // Using DPL?
  if (mDplChannelName != "") {
    mStandalone = false;
    mDplEnabled = true;
    DDLOG(fair::Severity::info) << "DPL Channel name: " << mDplChannelName;
  } else {
    mDplEnabled = false;
    DDLOG(fair::Severity::info) << "Not sending to DPL.";
  }

  // check if output enabled
  if (mStandalone && !mFileSink.enabled()) {
    DDLOG(fair::Severity::WARNING) << "Running in standalone mode and with STF file sink disabled. "
                    "Data will be lost.";
  }

    // channel for FileSource: stf or dpl, or generic one in case of standalone
  if (mStandalone) {
    // create default FMQ shm channel
    auto lTransportFactory = FairMQTransportFactory::CreateTransportFactory("shmem", "", GetConfig());
    if (!lTransportFactory) {
      DDLOG(fair::Severity::ERROR) << "Creating transport factory failed!";
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
  }

  DDLOG(fair::Severity::info) << "Sending data to channel: " << getOutputChannel().GetName();
}

void StfBuilderDevice::PreRun()
{
  // try to see if channels have been configured
  {
    if (!mFileSource.enabled()) {
      try {
        GetChannel(mInputChannelName);
      } catch(std::exception &) {
        DDLOG(fair::Severity::ERROR) << "Input channel not configured (from readout.exe) and not running with file source enabled.";
        exit(-1);
      }
    }

    try {
      if (!mStandalone) {
        GetChannel(mDplEnabled ? mDplChannelName : mOutputChannelName);
      }
    } catch(std::exception &) {
      DDLOG(fair::Severity::ERROR) << "Output channel not configured (to DPL or StfSender) and not running in standalone mode.";
      exit(-1);
    }
  }

  // start output thread
  mOutputThread = std::thread(&StfBuilderDevice::StfOutputThread, this);
  // start file sink
  mFileSink.start();

  // start file source
  // channel for FileSource: stf or dpl, or generic one in case of standalone
  mFileSource.start(getOutputChannel(), mDplEnabled);

  // start a thread for readout process
  if (!mFileSource.enabled()) {
    mReadoutInterface.start(1, mDataOrigin);
  }

  // info thread
  mInfoThread = std::thread(&StfBuilderDevice::InfoThread, this);

  DDLOG(fair::Severity::info) << "PreRun() done... ";
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

  // wait for the info thread
  if (mInfoThread.joinable()) {
    mInfoThread.join();
  }

  DDLOGF(fair::Severity::trace, "ResetTask() done... ");
}

void StfBuilderDevice::StfOutputThread()
{
  // wait for the device to go into RUNNING state
  WaitForRunningState();

  std::unique_ptr<InterleavedHdrDataSerializer> lStfSerializer;
  std::unique_ptr<StfToDplAdapter> lStfDplAdapter;

  // cannot get the channels in standalone mode
  auto& lOutputChan = getOutputChannel();

  DDLOG(fair::Severity::info) << "StfOutputThread: sending data to channel: " << lOutputChan.GetName();

  if (!mStandalone) {
    if (!dplEnabled()) {
      // auto& lOutputChan = GetChannel(getOutputChannelName(), 0);
      lStfSerializer = std::make_unique<InterleavedHdrDataSerializer>(lOutputChan);
    } else {
      // auto& lOutputChan = GetChannel(getDplChannelName(), 0);
      lStfDplAdapter = std::make_unique<StfToDplAdapter>(lOutputChan);
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

    {
      static thread_local unsigned long lThrottle = 0;
      if (lThrottle++ % 88 == 0) {
        DDLOG(fair::Severity::DEBUG) << "Sending STF::id:" << lStf->header().mId
          << " to channel: " << lOutputChan.GetName()
          << ", data size: " << lStf->getDataSize()
          << ", unique equipments: " << lStf->getEquipmentIdentifiers().size();
      }
    }

    // get data size sample
    mStfSizeSamples.Fill(lStf->getDataSize());

    if (!mStandalone) {
      const auto lSendStartTime = hres_clock::now();

      try {

        if (!dplEnabled()) {
          assert (lStfSerializer);
          lStfSerializer->serialize(std::move(lStf));
        } else {
          // Send to DPL bridge
          assert (lStfDplAdapter);
          lStfDplAdapter->sendToDpl(std::move(lStf));
        }
      } catch (std::exception& e) {
        if (IsRunningState()) {
          DDLOG(fair::Severity::ERROR) << "StfOutputThread: exception on send: " << e.what();
        } else {
          DDLOG(fair::Severity::info) << "StfOutputThread(NOT_RUNNING): shutting down: " << e.what();
        }
        break;
      }

      // record time spent in sending
      double lTimeMs = std::chrono::duration<double, std::milli>(hres_clock::now() - lSendStartTime).count();
      mStfDataTimeSamples.Fill(lTimeMs);
    }
  }

  DDLOG(fair::Severity::info) << "Exiting StfOutputThread...";
}

void StfBuilderDevice::InfoThread()
{
  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {

    DDLOGF(fair::Severity::info, "SubTimeFrame size_mean={} frequency_mean={} sending_time_ms_mean={} queued_stf={}",
      mStfSizeSamples.Mean(), mReadoutInterface.StfFreqSamples().Mean(), mStfDataTimeSamples.Mean(), mNumStfs);

    std::this_thread::sleep_for(2s);
  }
  DDLOG(fair::Severity::trace) << "Exiting Info thread...";
}

bool StfBuilderDevice::ConditionalRun()
{
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);
  return true;
}

bpo::options_description StfBuilderDevice::getDetectorProgramOptions() {
  bpo::options_description lDetectorOptions("SubTimeFrameBuilder data source", 120);

  lDetectorOptions.add_options() (
    OptionKeyStfDetector,
    bpo::value<std::string>()->default_value(""),
    "Specifies the detector string for SubTimeFrame building. Allowed are: "
    "ACO, CPV, CTP, EMC, FT0, FV0, FDD, HMP, ITS, MCH, MFT, MID, PHS, TOF, TPC, TRD, ZDC."
  )(
    OptionKeyRhdVer,
    bpo::value<ReadoutDataUtils::RdhVersion>()->default_value(ReadoutDataUtils::RdhVersion::eRdhInvalid, ""),
    "Specifies the version of RDH. Supported versions of the RDH are: 3, 4, 5, 6."
  )(
    OptionKeySubSpec,
    bpo::value<ReadoutDataUtils::SubSpecMode>()->default_value(ReadoutDataUtils::SubSpecMode::eCruLinkId, "cru_linkid"),
    "Specifies the which RDH fields are used for O2 Subspecification field: Allowed are:"
    "'cru_linkid' or 'feeid'."
  );

  return lDetectorOptions;
}

bpo::options_description StfBuilderDevice::getStfBuildingProgramOptions() {
  bpo::options_description lStfBuildingOptions("Options controlling SubTimeFrame building", 120);

  lStfBuildingOptions.add_options() (
    OptionKeyRdhSanityCheck,
    bpo::value<ReadoutDataUtils::SanityCheckMode>()->default_value(ReadoutDataUtils::SanityCheckMode::eNoSanityCheck, "off"),
    "Enable extensive RDH verification. Permitted values: off, print, drop (caution, any data not meeting criteria will be dropped)")(
    OptionKeyFilterEmptyTriggerData,
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
