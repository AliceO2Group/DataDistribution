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

#include "StfSenderDevice.h"

#include <ConfigConsul.h>

#include <SubTimeFrameDataModel.h>
#include <SubTimeFrameVisitors.h>
#include <SubTimeFrameDPL.h>

#include <options/FairMQProgOptions.h>

#include <chrono>
#include <thread>

namespace o2::DataDistribution
{

using namespace std::chrono_literals;

StfSenderDevice::StfSenderDevice()
  : DataDistDevice()
{
}

StfSenderDevice::~StfSenderDevice()
{ }

void StfSenderDevice::Init()
{
  DDDLOG("StfSenderDevice::Init()");
  mI = std::make_unique<StfSenderInstance>();

  I().mFileSink = std::make_unique<SubTimeFrameFileSink>(*this, *mI, eFileSinkIn, eFileSinkOut);
  I().mOutputHandler = std::make_unique<StfSenderOutput>(*this, *mI);
  I().mStandalone = GetConfig()->GetValue<bool>(OptionKeyStandalone);

  I().mPartitionId = Config::getPartitionOption(*GetConfig()).value_or("");
  if (I().mPartitionId.empty()) {
    WDDLOG("StfSender 'discovery-partition' parameter not set during Init(). Exiting.");
    ChangeState(fair::mq::Transition::ErrorFound);
    return;
  }

  I().mRpcServer = std::make_unique<StfSenderRpcImpl>(I().mPartitionId);

  // start monitoring
  DataDistMonitor::start_datadist(o2::monitoring::tags::Value::StfSender, GetConfig()->GetProperty<std::string>("monitoring-backend"));
  DataDistMonitor::set_interval(GetConfig()->GetValue<float>("monitoring-interval"));
  DataDistMonitor::set_log(GetConfig()->GetValue<bool>("monitoring-log"));
  // enable monitoring
  DataDistMonitor::enable_datadist(0, I().mPartitionId);

  // Info thread
  I().mInfoThread = create_thread_member("stfs_info", &StfSenderDevice::InfoThread, this);

  { // Discovery
    const bool lConsulRequired = !standalone();
    I().mDiscoveryConfig = std::make_shared<ConsulStfSender>(ProcessType::StfSender, Config::getEndpointOption(*GetConfig()), lConsulRequired);

    if (I().mDiscoveryConfig->enabled()) {
      auto& lStatus = I().mDiscoveryConfig->status();
      lStatus.mutable_info()->set_type(StfSender);
      lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
      lStatus.mutable_info()->set_process_id(Config::getIdOption(StfSender, *GetConfig(), lConsulRequired));
      lStatus.mutable_info()->set_ip_address(Config::getNetworkIfAddressOption(*GetConfig()));
      lStatus.mutable_partition()->set_partition_id(I().mPartitionId);
      I().mDiscoveryConfig->write();
    }
  }

  { // start the RPC server
    if (!standalone()) {
      // Start output handler
      I().mOutputHandler->start(I().mDiscoveryConfig);

      int lRpcRealPort = 0;
      auto& lStatus = I().mDiscoveryConfig->status();
      I().mRpcServer->start(I().mOutputHandler.get(), lStatus.info().ip_address(), lRpcRealPort);
      lStatus.set_rpc_endpoint(lStatus.info().ip_address() + ":" + std::to_string(lRpcRealPort));
      I().mDiscoveryConfig->write();
    }
  }
}

void StfSenderDevice::Reset()
{
  DDDLOG("StfBuilderDevice::Reset()");

  I().mDeviceRunning = false;
  // wait the Info thread, before closing mTfSchedulerRpcClient
  if (I().mInfoThread.joinable()) {
    I().mInfoThread.join();
  }

  // clear all Stfs from the pipeline before the transport is deleted
  if (mI) {
    I().stopPipeline();
    I().clearPipeline();
    mI.reset();
  }

  // stop monitoring
  DataDistMonitor::stop_datadist();
}

void StfSenderDevice::InitTask()
{
  DataDistLogger::SetThreadName("stfs-main");

  // Not available in Init()
  if (fair::mq::Transport::SHM != Transport()->GetType()) {
    EDDLOG("Default transport parameter must be set to shm.");
    ChangeState(fair::mq::Transition::ErrorFound);
    return;
  }

  I().mInputChannelName = GetConfig()->GetValue<std::string>(OptionKeyInputChannelName);

  try {
    GetChannel(I().mInputChannelName, 0);
  } catch (...) {
    EDDLOG("Requested input channel is not configured. input_chan={}", I().mInputChannelName);
    ChangeState(fair::mq::Transition::ErrorFound);
    return;
  }

  // File sink
  if (!I().mFileSink->loadVerifyConfig(*GetConfig())) {
    ChangeState(fair::mq::Transition::ErrorFound);
    return;
  }

  // check if any outputs enabled
  if (standalone() && !I().mFileSink->enabled()) {
    WDDLOG("Running in standalone mode and with STF file sink disabled. Data will be lost.");
  }

  {
    I().mRunning = true;

    if (!standalone()) {
      // register shm regions for ucx. Must be done from InitTask()!
      I().mOutputHandler->register_regions();

      // contact the scheduler on gRPC
      while (I().mTfSchedulerRpcClient.should_retry_start() && !I().mTfSchedulerRpcClient.start(I().mDiscoveryConfig)) {
        std::this_thread::sleep_for(150ms);
      }

      // Did we fail to connect to the TfScheduler?
      if (!I().mTfSchedulerRpcClient.should_retry_start()) {
        EDDLOG("InitTask: Failed to connect to TfScheduler. Exiting.");
        ChangeState(fair::mq::Transition::ErrorFound);
        return;
      }
    } else {
      // Start output handler for standalone
      I().mOutputHandler->start_standalone(I().mDiscoveryConfig);
    }

    // start file sink
    if (I().mFileSink->enabled()) {
      I().mFileSink->start();
    }

    // start the receiver thread
    I().mReceiverThread = create_thread_member("stfs_recv", &StfSenderDevice::StfReceiverThread, this);
  }
}

void StfSenderDevice::PreRun()
{
  // update running state
  if (!standalone() && I().mDiscoveryConfig) {
    auto& lStatus = I().mDiscoveryConfig->status();
    lStatus.mutable_info()->set_process_state(BasicInfo::RUNNING);
    lStatus.mutable_partition()->set_run_number(DataDistLogger::sRunNumber);
    I().mDiscoveryConfig->write();
  }

  // reset counters
  if (I().mOutputHandler) {
    I().mOutputHandler->resetCounters();
  }

  // make directory for file sink
  if (I().mFileSink) {
    I().mFileSink->makeDirectory();
  }

  // enable monitoring
  DataDistMonitor::enable_datadist(DataDistLogger::sRunNumber, I().mPartitionId);

  // start accepting data
  I().mAcceptingData = true;
  I().mLastStfId = 0;

  IDDLOG("Entering running state. RunNumber: {}", DataDistLogger::sRunNumberStr);
}

void StfSenderDevice::PostRun()
{
  // stop accepting data
  I().mAcceptingData = false;
  I().mLastStfId = ~uint64_t(0);

  // update running state
  if (!standalone() && I().mDiscoveryConfig) {
    auto& lStatus = I().mDiscoveryConfig->status();
    lStatus.mutable_info()->set_process_state(BasicInfo::NOT_RUNNING);
    I().mDiscoveryConfig->write();
  }

  IDDLOG("Exiting running state. RunNumber: {}", DataDistLogger::sRunNumberStr);
}

void StfSenderDevice::ResetTask()
{
  I().mRunning = false;

    // Stop the pipeline
  I().stopPipeline();

  // stop the receiver thread
  if (I().mReceiverThread.joinable()) {
    I().mReceiverThread.join();
  }
  I().clearPipeline();

  // stop file sink
  if (I().mFileSink->enabled()) {
    I().mFileSink->stop();
  }

  if (!standalone()) {
    // Stop the RPC server after output
    I().mRpcServer->stop();

    // Stop output handler
    I().mOutputHandler->stop();

    // Stop the Scheduler RPC client
    I().mTfSchedulerRpcClient.stop();
  }

  DDDLOG("ResetTask() done.");
}

void StfSenderDevice::StfReceiverThread()
{
  auto& lInputChan = GetChannel(I().mInputChannelName, 0);

  // InterleavedHdrDataDeserializer lStfReceiver;
  DplToStfAdapter  lStfReceiver;
  std::unique_ptr<SubTimeFrame> lStf;

  DDMON_RATE("stfsender", "stf_input", 0.0);

  while (running()) {
    try {
      lStf = lStfReceiver.deserialize(lInputChan, acceptingData());
    } catch (const std::exception &e) {
      EDDLOG_RL(5000, "StfSender: received STF cannot be deserialized. what={}", e.what());
      continue;
    } catch (...) {
      EDDLOG_RL(5000, "StfSender: received STF cannot be deserialized. what=UNKNOWN");
      continue;
    }

    if (!acceptingData()) {
      if (lStf) {
        WDDLOG_RL(2000, "StfSender: received STF but not in the running state.");
      }
      continue;
    }

    if (!lStf) { // we're running but there is no STFs incoming
      if (I().mLastStfId != ~uint64_t(0)) {
        DDMON("stfsender", "stf_input.id", I().mLastStfId);
      }
      continue;
    }

    DDDLOG_RL(2000, "StfReceiverThread:: SubTimeFrame stf_id={} size={} unique_equip={}",
      lStf->header().mId, lStf->getDataSize(), lStf->getEquipmentIdentifiers().size());

    const auto lStfDelay = std::chrono::time_point_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now()).time_since_epoch().count()/1000.0 - lStf->header().mCreationTimeMs;

    I().mLastStfId = lStf->id();

    DDMON_RATE("stfsender", "stf_input", lStf->getDataSize());
    DDMON("stfsender", "stf_input.id", I().mLastStfId);
    DDMON("stfsender", "stf_input.delay_ms", lStfDelay);

    if (lStfDelay > 500.0) {
      WDDLOG_RL(5000, "Large delay of STFs on arrival to StfSender. delay_ms={:4f}", lStfDelay);
    }

    I().queue(eReceiverOut, std::move(lStf));
  }

  DDDLOG("Exiting StfReceiverThread.");
}

void StfSenderDevice::InfoThread()
{
  while (deviceRunning()) {
    DDMON("stfsender", "fmq.state", +GetCurrentState());

    std::this_thread::sleep_for(500ms);
  }
  DDDLOG("Exiting Info thread.");
}

bool StfSenderDevice::ConditionalRun()
{
  if (I().mRpcServer->isTerminateRequested()) {
    IDDLOG_RL(10000, "DataDistribution partition is terminated.");
    return false; // trigger PostRun()
  }

  if (running() && !standalone() && I().mDiscoveryConfig) {
    const auto lInfo = I().mDiscoveryConfig->status().info();
    I().mTfSchedulerRpcClient.HeartBeat(lInfo);
  }

  // nothing to do here sleep for awhile
  std::this_thread::sleep_for(500ms);
  return true;
}

} /* namespace o2::DataDistribution */
