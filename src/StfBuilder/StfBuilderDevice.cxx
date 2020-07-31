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
    IFifoPipeline(eStfPipelineSize)
{
}

StfBuilderDevice::~StfBuilderDevice()
{
}

void StfBuilderDevice::Init()
{
  mI = std::make_unique<StfBuilderInstance>();

  I().mFileSource = std::make_unique<SubTimeFrameFileSource>(*this, eStfFileSourceOut);
  I().mReadoutInterface = std::make_unique<StfInputInterface>(*this);
  I().mFileSink = std::make_unique<SubTimeFrameFileSink>(*this, *this, eStfFileSinkIn, eStfFileSinkOut);
}

void StfBuilderDevice::Reset()
{
  // clear all Stfs from the pipeline before the transport is deleted
  clearPipeline();

  mI.reset();
}

void StfBuilderDevice::InitTask()
{
  DataDistLogger::SetThreadName("stfb-main");

  I().mInputChannelName = GetConfig()->GetValue<std::string>(OptionKeyInputChannelName);
  I().mOutputChannelName = GetConfig()->GetValue<std::string>(OptionKeyOutputChannelName);
  I().mDplChannelName = GetConfig()->GetValue<std::string>(OptionKeyDplChannelName);
  I().mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);
  I().mMaxStfsInPipeline = GetConfig()->GetValue<std::int64_t>(OptionKeyMaxBufferedStfs);

  // input data handling
  ReadoutDataUtils::sSpecifiedDataOrigin = getDataOriginFromOption(
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
  if (I().mMaxStfsInPipeline > 0) {
    if (I().mMaxStfsInPipeline < 4) {
      I().mMaxStfsInPipeline = 4;
      DDLOGF(fair::Severity::WARNING, "Configuration: max buffered SubTimeFrames limit increased to {}",
        I().mMaxStfsInPipeline);
    }
    I().mPipelineLimit = true;
    DDLOGF(fair::Severity::WARNING, "Configuration: Max buffered SubTimeFrames limit is set to {}."
      " Consider increasing it if data loss occurs.", I().mMaxStfsInPipeline);
  } else {
    I().mPipelineLimit = false;
    DDLOGF(fair::Severity::info, "Not imposing limits on number of buffered SubTimeFrames. "
      "Possibility of creating back-pressure.");
  }

  // File sink
  if (!I().mFileSink->loadVerifyConfig(*(this->GetConfig()))) {
    exit(-1);
  }

  // File source
  if (!I().mFileSource->loadVerifyConfig(*(this->GetConfig()))) {
    exit(-1);
  }

  // make sure we have detector if not using files
  if (!I().mFileSource->enabled()) {
    if ((ReadoutDataUtils::sRdhVersion < ReadoutDataUtils::RdhVersion::eRdhVer6) &&
      (ReadoutDataUtils::sSpecifiedDataOrigin == gDataOriginInvalid)) {
      DDLOGF(fair::Severity::ERROR, "Detector string parameter must be specified when receiving the data from the "
        "readout and not using RDHv6 or greater.");
      exit(-1);
    } else {
      DDLOGF(fair::Severity::info, "READOUT INTERFACE: Configured detector: {}",
        ReadoutDataUtils::sSpecifiedDataOrigin.str);
    }

    if (ReadoutDataUtils::sRdhVersion == ReadoutDataUtils::RdhVersion::eRdhInvalid) {
      DDLOGF(fair::Severity::FATAL, "RDH version must be specified when receiving the data from the readout.");
      exit(-1);
    } else {
      DDLOGF(fair::Severity::info, "READOUT INTERFACE: Configured RDHv{}", ReadoutDataUtils::sRdhVersion);
      RDHReader::Initialize(unsigned(ReadoutDataUtils::sRdhVersion));
    }

    DDLOGF(fair::Severity::info, "READOUT INTERFACE: Configured O2 SubSpec mode: {}", to_string(ReadoutDataUtils::sRawDataSubspectype));

    if (ReadoutDataUtils::sRdhSanityCheckMode != ReadoutDataUtils::SanityCheckMode::eNoSanityCheck) {
      DDLOGF(fair::Severity::info, "Extensive RDH checks enabled. Data that does not meet the criteria will be {}.",
        (ReadoutDataUtils::sRdhSanityCheckMode == ReadoutDataUtils::eSanityCheckDrop ? "dropped" : "kept"));
    }

    if (ReadoutDataUtils::sEmptyTriggerHBFrameFilterring) {
      DDLOGF(fair::Severity::info, "Filtering of empty HBFrames in triggered mode enabled.");
    }
  }

  // Using DPL?
  if (I().mDplChannelName != "" && !I().mStandalone) {
    I().mDplEnabled = true;
    DDLOGF(fair::Severity::info, "DPL Channel name: {}",  I().mDplChannelName);
  } else {
    I().mDplEnabled = false;
    I().mDplChannelName = "";
    DDLOGF(fair::Severity::info, "Not sending data to DPL.");
  }

  // check if output enabled
  if (isSandalone() && !I().mFileSink->enabled()) {
    DDLOGF(fair::Severity::WARNING, "Running in standalone mode and with STF file sink disabled. Data will be lost.");
  }

    // channel for FileSource: stf or dpl, or generic one in case of standalone
  if (isSandalone()) {
    // create default FMQ shm channel
    auto lTransportFactory = FairMQTransportFactory::CreateTransportFactory("shmem", "", GetConfig());
    if (!lTransportFactory) {
      DDLOGF(fair::Severity::ERROR, "Creating transport factory failed!");
      exit(-1);
    }

    I().mStandaloneChannel = std::make_unique<FairMQChannel>(
      "standalone-chan[0]" ,  // name
      "pair",                 // type
      "bind",                 // method
      "ipc:///tmp/standalone-chan-stfb", // address
      lTransportFactory
    );

    // mStandaloneChannel.Init();
    I().mStandaloneChannel->Init();
    // mStandaloneChannel->BindEndpoint("ipc:///tmp/standalone-chan");
    I().mStandaloneChannel->Validate();
  }

  DDLOGF(fair::Severity::info, "Sending data to channel: {}", getOutputChannel().GetName());

  // try to see if channels have been configured
  {
    if (!I().mFileSource->enabled()) {
      try {
        GetChannel(I().mInputChannelName);
      } catch(std::exception &) {
        DDLOGF(fair::Severity::ERROR, "Input channel not configured (from readout.exe) and not running with file source enabled.");
        exit(-1);
      }
    }

    try {
      if (!isSandalone()) {
        GetChannel(I().mDplEnabled ? I().mDplChannelName : I().mOutputChannelName);
      }
    } catch(std::exception &) {
      DDLOGF(fair::Severity::ERROR, "Output channel (to DPL or StfSender) must be configured if not running in stand-alone mode.");
      exit(-1);
    }
  }

  // start output thread
  I().mOutputThread = std::thread(&StfBuilderDevice::StfOutputThread, this);
  // start file sink
  I().mFileSink->start();

  // start file source
  // channel for FileSource: stf or dpl, or generic one in case of standalone
  I().mFileSource->start(getOutputChannel(), I().mDplEnabled);

  // start a thread for readout process
  if (!I().mFileSource->enabled()) {
    I().mReadoutInterface->start(1);
  }

  // info thread
  I().mInfoThread = std::thread(&StfBuilderDevice::InfoThread, this);

  DDLOGF(fair::Severity::info, "PreRun() done... ");
}

void StfBuilderDevice::ResetTask()
{
  // signal and wait for the output thread
  if (I().mFileSource->enabled()) {
    I().mFileSource->stop();
  }

  // Stop the pipeline
  stopPipeline();

  // wait for readout interface threads
  if (!I().mFileSource->enabled()) {
    I().mReadoutInterface->stop();
  }

  // signal and wait for the output thread
  I().mFileSink->stop();

  // stop the output
  if (I().mOutputThread.joinable()) {
    I().mOutputThread.join();
  }

  // wait for the info thread
  if (I().mInfoThread.joinable()) {
    I().mInfoThread.join();
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

  DDLOGF(fair::Severity::info, "StfOutputThread: sending data to channel: {}", lOutputChan.GetName());

  if (!isSandalone()) {
    if (!dplEnabled()) {
      lStfSerializer = std::make_unique<InterleavedHdrDataSerializer>(lOutputChan);
    } else {
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
    I().mNumStfs--;

    DDLOGF_RL(2000, fair::Severity::DEBUG, "Sending an STF out. stf_id={} channel={} stf_size={} unique_equipments={}",
      lStf->header().mId, lOutputChan.GetName(), lStf->getDataSize(), lStf->getEquipmentIdentifiers().size());

    // get data size sample
    I().mStfSizeSamples.Fill(lStf->getDataSize());

    if (!isSandalone()) {
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
          DDLOGF(fair::Severity::ERROR, "StfOutputThread: exception on send: what={}", e.what());
        } else {
          DDLOGF(fair::Severity::info, "StfOutputThread(NOT_RUNNING): shutting down: what={}", e.what());
        }
        break;
      }

      // record time spent in sending
      static const auto sStartOfStfSending = hres_clock::now();
      I().mSentOutStfs++;
      const auto lNow = hres_clock::now();
      const double lTimeMs = std::max(1e-6, std::chrono::duration<double, std::milli>(lNow - lSendStartTime).count());
      I().mSentOutRate = double(I().mSentOutStfs) / std::chrono::duration<double>(lNow - sStartOfStfSending).count();
      I().mStfDataTimeSamples.Fill(lTimeMs);
    } else {
      // DDLOGF(fair::Severity::ERROR, "Dropping stf size={}", lStf->getDataSize());
    }
  }

  DDLOGF(fair::Severity::info, "Exiting StfOutputThread...");
}

void StfBuilderDevice::InfoThread()
{
  // wait for the device to go into RUNNING state
  WaitForRunningState();

  while (IsRunningState()) {

    DDLOGF(fair::Severity::info, "SubTimeFrame size_mean={} frequency_mean={} sending_time_ms_mean={} queued_stf={}",
      I().mStfSizeSamples.Mean(), I().mReadoutInterface->StfFreqSamples().Mean(), I().mStfDataTimeSamples.Mean(),
      I().mNumStfs);
    DDLOGF(fair::Severity::info, "SubTimeFrame sent_total={} rate_total={:.3}", I().mSentOutStfs, I().mSentOutRate);

    std::this_thread::sleep_for(2s);
  }
  DDLOGF(fair::Severity::trace, "Exiting Info thread...");
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
