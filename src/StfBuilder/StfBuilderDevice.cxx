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

#include <DataDistLogger.h>
#include <SubTimeFrameVisitors.h>
#include <ReadoutDataModel.h>
#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameDPL.h>
#include <Utilities.h>
#include <ConfigConsul.h>

#include <Headers/DataHeader.h>

#include <fairmq/ProgOptions.h>

#include <chrono>
#include <thread>
#include <exception>
#include <boost/algorithm/string.hpp>


namespace o2::DataDistribution
{

using namespace std::chrono_literals;

constexpr int StfBuilderDevice::gStfOutputChanId;

StfBuilderDevice::StfBuilderDevice()
  : DataDistDevice()
{
}

static bool sResetDeviceCalled = false;

StfBuilderDevice::~StfBuilderDevice()
{
  DDDLOG("StfBuilderDevice::~StfBuilderDevice()");

  if (!sResetDeviceCalled) {
    DDDLOG("StfBuilderDevice::Reset() was not called. Performing cleanup");
    // clear all Stfs from the pipeline before the transport is deleted
    if (mI) {
      I().stopPipeline();
      I().clearPipeline();
      mI.reset();
    }

    mMemI.reset();
  }
}

void StfBuilderDevice::Init()
{
  DDDLOG("StfBuilderDevice::Init()");
  mI = std::make_unique<StfBuilderInstance>();
  mMemI = std::make_unique<SyncMemoryResources>(this->AddTransport(fair::mq::Transport::SHM));

  I().mFileSource = std::make_unique<SubTimeFrameFileSource>(*mI, eStfFileSourceOut);
  I().mReadoutInterface = std::make_unique<StfInputInterface>(*this);
  I().mFileSink = std::make_unique<SubTimeFrameFileSink>(*this, *mI, eStfFileSinkIn, eStfFileSinkOut);
}

void StfBuilderDevice::Reset()
{
  DDDLOG("StfBuilderDevice::Reset()");
  // clear all Stfs from the pipeline before the transport is deleted
  I().stopPipeline();
  I().clearPipeline();

  mI.reset();
  mMemI.reset();

  sResetDeviceCalled = true;
}

void StfBuilderDevice::InitTask()
{
  DataDistLogger::SetThreadName("stfb-main");

  I().mInputChannelName = GetConfig()->GetValue<std::string>(OptionKeyInputChannelName);
  I().mOutputChannelName = GetConfig()->GetValue<std::string>(OptionKeyOutputChannelName);
  I().mDplChannelName = GetConfig()->GetValue<std::string>(OptionKeyDplChannelName);
  I().mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);
  I().mMaxStfsInPipeline = GetConfig()->GetValue<std::int64_t>(OptionKeyMaxBufferedStfs);
  I().mMaxBuiltStfs = GetConfig()->GetValue<std::uint64_t>(OptionKeyMaxBuiltStfs);

  // partition id
  I().mPartitionId = Config::getPartitionOption(*GetConfig()).value_or("");
  if (I().mPartitionId.empty() && I().mStandalone) {
    I().mPartitionId = "STAND-ALONE";
  }
  if (I().mPartitionId.empty()) {
    EDDLOG("Partition id is not provided during InitTask(). Check command line or ECS parameters. Exiting.");
    ChangeState(fair::mq::Transition::ErrorFound);
    return;
  }

  // start monitoring
  DataDistMonitor::start_datadist(o2::monitoring::tags::Value::StfBuilder, GetConfig()->GetProperty<std::string>("monitoring-backend", ""));
  DataDistMonitor::set_interval(GetConfig()->GetValue<float>("monitoring-interval"));
  DataDistMonitor::set_log(GetConfig()->GetValue<bool>("monitoring-log"));
  if (auto lInterval = GetConfig()->GetValue<int>("monitoring-process-interval"); lInterval >= 0) {
    DataDistMonitor::enable_process_monitoring(lInterval > 0 ? std::optional<unsigned>(lInterval) : std::nullopt);
  }

  // input data handling
  ReadoutDataUtils::sSpecifiedDataOrigin = getDataOriginFromOption(
    GetConfig()->GetValue<std::string>(OptionKeyStfDetector));

  ReadoutDataUtils::sRdhVersion =
    GetConfig()->GetValue<ReadoutDataUtils::RdhVersion>(OptionKeyRhdVer);

  ReadoutDataUtils::sRawDataSubspectype =
    GetConfig()->GetValue<ReadoutDataUtils::SubSpecMode>(OptionKeySubSpec);

  ReadoutDataUtils::sRunType = GetConfig()->GetValue<ReadoutDataUtils::RunType>(OptionKeyRunType);

  ReadoutDataUtils::sRdhSanityCheckMode =
    GetConfig()->GetValue<ReadoutDataUtils::SanityCheckMode>(OptionKeyRdhSanityCheck);

  ReadoutDataUtils::sEmptyTriggerHBFrameFilterring =
    GetConfig()->GetValue<bool>(OptionKeyFilterEmptyTriggerData);

  // check run type
  if (ReadoutDataUtils::sRunType == ReadoutDataUtils::RunType::eInvalid) {
    EDDLOG("Run type paramter must be correctly set.");
    ChangeState(fair::mq::Transition::ErrorFound);
    return;
  }

  // check run type
  if (ReadoutDataUtils::sRunType == ReadoutDataUtils::RunType::eTopology) {
    if (! ((ReadoutDataUtils::sSpecifiedDataOrigin == o2::header::gDataOriginITS) ||
      (ReadoutDataUtils::sSpecifiedDataOrigin == o2::header::gDataOriginMFT))) {
      EDDLOG("Run type paramter 'topology' is supported only for ITS and MFT. Please specify the detector option. detector={}",
        ReadoutDataUtils::sSpecifiedDataOrigin.as<std::string>());
      ChangeState(fair::mq::Transition::ErrorFound);
      return;
    }
  }

  IDDLOG("Configuration: Run type: '{}'", to_string(ReadoutDataUtils::sRunType));

  // Buffering limitation
  if (I().mMaxStfsInPipeline > 0) {
    if (I().mMaxStfsInPipeline < 4) {
      I().mMaxStfsInPipeline = 4;
      WDDLOG("Configuration: max buffered SubTimeFrames limit increased to {}", I().mMaxStfsInPipeline);
    }
    I().mPipelineLimit = true;
    WDDLOG("Configuration: Max buffered SubTimeFrames limit is set to {}."
      " Consider increasing it if data loss occurs.", I().mMaxStfsInPipeline);
  } else {
    I().mPipelineLimit = false;
    IDDLOG("Not imposing limits on number of buffered SubTimeFrames. Possibility of creating back-pressure.");
  }

  // Limited number of STF?
  IDDLOG("Configuration: Number of built SubTimeFrames is {}",
    I().mMaxBuiltStfs == 0 ? "not limited" : ("limited to " + std::to_string(I().mMaxBuiltStfs)));

  // File sink
  if (!I().mFileSink->loadVerifyConfig(*(this->GetConfig()))) {
    ChangeState(fair::mq::Transition::ErrorFound);
    return;
  }

  // File source
  if (!I().mFileSource->loadVerifyConfig(*(this->GetConfig()))) {
    ChangeState(fair::mq::Transition::ErrorFound);
    return;
  }

  // Discovery. Verify other parameters when running online and !standalone
  {
    // Require consul and discovery options when not using file source or running standalone
    const bool lConsulRequired = !(I().mFileSource->enabled() || I().mStandalone);
    const auto lConsulEndpoint = Config::getEndpointOption(*GetConfig());
    I().mDiscoveryConfig = std::make_shared<ConsulStfBuilder>(ProcessType::StfBuilder, lConsulEndpoint, lConsulRequired);
    if (I().mDiscoveryConfig->enabled()) {
      auto& lStatus = I().mDiscoveryConfig->status();
      lStatus.mutable_info()->set_type(StfBuilder);
      lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
      lStatus.mutable_info()->set_process_id(Config::getIdOption(StfBuilder, *GetConfig(), lConsulRequired));
      lStatus.mutable_partition()->set_partition_id(I().mPartitionId);
      I().mDiscoveryConfig->write();
    }
  }

  I().mState.mRunning = true;

  // make sure we have detector if not using files
  if (!I().mFileSource->enabled()) {
    if ((ReadoutDataUtils::sRdhVersion < ReadoutDataUtils::RdhVersion::eRdhVer6) &&
      (ReadoutDataUtils::sSpecifiedDataOrigin == o2::header::gDataOriginAny)) {
      EDDLOG("Detector string parameter must be specified when receiving the data from the "
        "readout and not using RDHv6 or greater.");
      ChangeState(fair::mq::Transition::ErrorFound);
      return;
    } else {
      IDDLOG("READOUT INTERFACE: Configured detector: {}", ReadoutDataUtils::sSpecifiedDataOrigin.as<std::string>());
    }

    if (ReadoutDataUtils::sRdhVersion == ReadoutDataUtils::RdhVersion::eRdhInvalid) {
      EDDLOG("The RDH version must be specified when receiving data from readout.");
      ChangeState(fair::mq::Transition::ErrorFound);
      return;
    } else {
      IDDLOG("READOUT INTERFACE: Configured RDHv{}", ReadoutDataUtils::sRdhVersion);
      RDHReader::Initialize(unsigned(ReadoutDataUtils::sRdhVersion));
    }

    IDDLOG("READOUT INTERFACE: Configured O2 SubSpec mode: {}", to_string(ReadoutDataUtils::sRawDataSubspectype));

    if (ReadoutDataUtils::sRdhSanityCheckMode != ReadoutDataUtils::SanityCheckMode::eNoSanityCheck) {
      IDDLOG("Extensive RDH checks enabled. Data that does not meet the criteria will be {}.",
        (ReadoutDataUtils::sRdhSanityCheckMode == ReadoutDataUtils::eSanityCheckDrop ? "dropped" : "kept"));
    }

    if (ReadoutDataUtils::sEmptyTriggerHBFrameFilterring) {
      IDDLOG("Filtering of empty HBFrames in triggered mode enabled.");
    }
  }

  // Using DPL?
  if (I().mDplChannelName != "" && !I().mStandalone) {
    IDDLOG("DPL Channel name: {}",  I().mDplChannelName);
  } else {
    I().mDplChannelName = "";
    IDDLOG("Not sending data to DPL.");
  }

  // check if output enabled
  if (isStandalone() && !I().mFileSink->enabled()) {
    WDDLOG("Running in standalone mode and with STF file sink disabled. Data will be lost.");
  }

  // try to see if channels have been configured
  {
    if (!I().mFileSource->enabled()) {
      try {
        GetChannel(I().mInputChannelName);
      } catch(std::exception &) {
        EDDLOG("Input channel not configured (from o2-readout-exe) and not running with file source enabled.");
        ChangeState(fair::mq::Transition::ErrorFound);
        return;
      }
    }

    try {
      if (!isStandalone()) {
        GetChannel( !I().mDplChannelName.empty() ? I().mDplChannelName : I().mOutputChannelName);
      }
    } catch(std::exception &e) {
      EDDLOG("Output channel (to DPL or StfSender) must be configured if not running in stand-alone mode.");
      ChangeState(fair::mq::Transition::ErrorFound);
      return;
    }
  }

  // start output thread
  I().mOutputThread = create_thread_member("stfb_out", &StfBuilderDevice::StfOutputThread, this);
  // start file sink
  I().mFileSink->start();

  // start file source
  I().mFileSource->start(MemI());

  // start a thread for readout process
  if (!I().mFileSource->enabled()) {
    I().mReadoutInterface->start(ReadoutDataUtils::sRunType == ReadoutDataUtils::RunType::ePhysics, I().mDiscoveryConfig);
  }

  IDDLOG("InitTask() done... ");
}

void StfBuilderDevice::ResetTask()
{
  DDDLOG("StfBuilderDevice::ResetTask()");

  // Signal ConditionalRun() and other threads to stop
  I().mState.mRunning = false;

  // Stop the pipeline
  I().stopPipeline();
  I().clearPipeline();

  // NOTE: everything with threads goes below
  // signal and wait for the output thread
  if (I().mFileSource->enabled()) {
    I().mFileSource->stop();
  } else {
    I().mReadoutInterface->stop();
  }

  // stop the file sink
  if (I().mFileSink) {
    I().mFileSink->stop();
  }

  // signal and wait for the output thread
  if (I().mOutputThread.joinable()) {
    I().mOutputThread.join();
  }

  // stop the memory resources very last
  MemI().stop();

  DDDLOG("StfBuilderDevice::ResetTask() done... ");
}

void StfBuilderDevice::StfOutputThread()
{
  using hres_clock = std::chrono::high_resolution_clock;

  std::unique_ptr<StfToDplAdapter> lStfDplAdapter;

  bool lShouldSendEos = false;

  if (!isStandalone()) {
    // cannot get the channels in standalone mode
    auto& lOutputChan = getOutputChannel();
    IDDLOG("StfOutputThread: sending data to channel: {}", lOutputChan.GetName());

    lStfDplAdapter = std::make_unique<StfToDplAdapter>(lOutputChan, MemI());
  }

  DDMON_RATE("stfbuilder", "stf_output", 0.0);

  while (I().mState.mRunning) {
    // Get a STF ready for sending, or nullopt
    auto lStfOpt = I().dequeue_for(eStfSendIn, 100ms);
    if (!I().mState.mRunning) {
      break;
    } else if (!I().is_running(eStfSendIn)) {
      break;
    } else if (!I().mState.mInRunningState) {
      if (lStfOpt) {
        WDDLOG_RL(1000, "Dropping a raw SubTimeFrame because stop of the run is requested.");
      }

      if (lStfDplAdapter && lShouldSendEos) {
        lShouldSendEos = false;
        lStfDplAdapter->sendEosToDpl();
      }
      continue;
    }

    lShouldSendEos = true;
    if (lStfOpt == std::nullopt) {
      continue;
    }

    auto &lStf = lStfOpt.value();
    DDMON_RATE("stfbuilder", "stf_output", lStf->getDataSize());
    DDMON("stfbuilder", "stf_output.id", lStf->id());

    // decrement the stf counter
    I().mCounters.mNumStfs--;

    DDDLOG_RL(5000, "Sending an STF out. stf_id={} stf_size={} unique_equipments={}",
      lStf->id(), lStf->getDataSize(), lStf->getEquipmentIdentifiers().size());

    if (!isStandalone()) {
      try {
        assert (lStfDplAdapter);
        lStfDplAdapter->sendToDpl(std::move(lStf));

      } catch (std::exception& e) {
        if (IsReadyOrRunningState()) {
          EDDLOG_RL(10000, "StfOutputThread: exception on send: what={}", e.what());
        } else {
          IDDLOG_RL(10000, "StfOutputThread(NOT_RUNNING): shutting down: what={}", e.what());
        }
        continue;
      }

      // record time spent in sending
      static auto sStartOfStfSending = hres_clock::now();
      if (I().mRestartRateCounter) {
        sStartOfStfSending = hres_clock::now();
        I().mSentOutStfs = 0;
        I().mRestartRateCounter = false;
      }

      I().mSentOutStfs++;
      I().mSentOutStfsTotal++;
      DDMON("stfbuilder", "stf_output.total", I().mSentOutStfs);

      const auto lNow = hres_clock::now();
      I().mSentOutRate = double(I().mSentOutStfs) / std::chrono::duration<double>(lNow - sStartOfStfSending).count();
    }

    // check if we should exit:
    // 1. max number of stf set, or
    // 2. file reply used without loop parameter
    if ( (I().mMaxBuiltStfs > 0) && (I().mSentOutStfsTotal == I().mMaxBuiltStfs) ) {
      IDDLOG("Maximum number of sent SubTimeFrames reached. Exiting.");
      break;
    }
  }

  // leaving the output thread, send end of the stream info
  if (!isStandalone() && lStfDplAdapter && lShouldSendEos) {
    lStfDplAdapter->sendEosToDpl();
  }

  I().mState.mRunning = false; // trigger stop via CondRun()

  IDDLOG("Output: Stopped SubTimeFrame sending. sent_total={} rate={:.4}", I().mSentOutStfsTotal, I().mSentOutRate);
  DDDLOG("Exiting StfOutputThread...");
}

void StfBuilderDevice::PreRun()
{
  // update running state
  if (I().mDiscoveryConfig->enabled()) {
    auto& lStatus = I().mDiscoveryConfig->status();
    lStatus.mutable_info()->set_process_state(BasicInfo::RUNNING);
    lStatus.mutable_partition()->set_run_number(DataDistLogger::sRunNumber);
    I().mDiscoveryConfig->write();
  }

  if (I().mReadoutInterface) {
    I().mReadoutInterface->setRunningState(true);
  }

  I().mState.mPaused = false;
  if (I().mFileSource) {
    I().mFileSource->resume();
    DDDLOG("Restarting file source.");
  }
  I().mRestartRateCounter = true;

  // make directory for file sink
  if (I().mFileSink) {
    I().mFileSink->makeDirectory();
  }

  // enable monitoring
  DataDistMonitor::enable_datadist(DataDistLogger::sRunNumber, GetConfig()->GetProperty<std::string>("discovery-partition", "-"));

  IDDLOG("Entering running state. RunNumber: {}", DataDistLogger::sRunNumberStr);

  // In Running state
  I().mState.mInRunningState = true;
}

void StfBuilderDevice::PostRun()
{
  // Not in Running state
  I().mState.mInRunningState = false;

  if (I().mReadoutInterface) {
    I().mReadoutInterface->setRunningState(false);
  }

  I().mState.mPaused = true;
  if (I().mFileSource) {
    I().mFileSource->pause();
    DDDLOG("Pausing file source.");
  }

  // update running state
  if (I().mDiscoveryConfig->enabled()) {
    auto& lStatus = I().mDiscoveryConfig->status();
    lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
    I().mDiscoveryConfig->write();
  }

  IDDLOG("Exiting running state. RunNumber: {}", DataDistLogger::sRunNumberStr);
}

bool StfBuilderDevice::ConditionalRun()
{
  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);

  if (!I().mState.mRunning) {
    DDDLOG("ConditionalRun() returning false.");
  }

  return I().mState.mRunning;
}

bpo::options_description StfBuilderDevice::getDetectorProgramOptions() {
  bpo::options_description lDetectorOptions("SubTimeFrameBuilder data source", 120);

  lDetectorOptions.add_options() (
    OptionKeyRunType,
    bpo::value<ReadoutDataUtils::RunType>()->default_value(ReadoutDataUtils::RunType::ePhysics, "physics"),
    "Specifies the operation mode for STF building. Deafult 'physics' STF aggregation, or 'topology' for topology based data distribution."
    " Allowed values: 'physics'(default) or 'topology'."
  )(
    OptionKeyStfDetector,
    bpo::value<std::string>()->default_value(""),
    "Specifies the detector string for SubTimeFrame building. Allowed are: "
    "TST, CPV, CTP, EMC, FT0, FV0, FDD, HMP, ITS, MCH, MFT, MID, PHS, TOF, TPC, TRD, ZDC."
  )(
    OptionKeyRhdVer,
    bpo::value<ReadoutDataUtils::RdhVersion>()->default_value(ReadoutDataUtils::RdhVersion::eRdhInvalid, ""),
    "Specifies the version of RDH. Supported versions of the RDH are: 3, 4, 5, 6."
  )(
    OptionKeySubSpec,
    bpo::value<ReadoutDataUtils::SubSpecMode>()->default_value(ReadoutDataUtils::SubSpecMode::eCruLinkId, "feeid"),
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

  if (lDetStr == "CPV") {
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
  else if (lDetStr == "TST") {
    return o2::header::gDataOriginTST;
  }
  else if (lDetStr == "NIL") {
    return o2::header::gDataOriginInvalid;
  }

  return o2::header::gDataOriginAny;
}

} /* namespace o2::DataDistribution */
